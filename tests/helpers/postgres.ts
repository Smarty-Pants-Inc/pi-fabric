import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import pg from "pg";

/**
 * A private PostgreSQL 17 for one test file: its own initdb in a temp directory, a Unix socket in
 * a 0700 directory, no TCP listener. Tests skip when no binaries are found; set
 * PI_FABRIC_TEST_PG_BIN to the directory holding initdb and postgres.
 */
const CANDIDATES = [
  process.env.PI_FABRIC_TEST_PG_BIN,
  path.join(os.homedir(), ".local/share/smarty-dev/knowledge-pg/17.11/usr/lib/postgresql/17/bin"),
  "/usr/lib/postgresql/17/bin",
  "/usr/local/opt/postgresql@17/bin",
  "/opt/homebrew/opt/postgresql@17/bin",
].filter((entry): entry is string => Boolean(entry));

const libraryPath = (bin: string): string | undefined => {
  // The knowledge-pg tree is an unpacked .deb: its libpq sits beside, not in the system path.
  const usrLib = path.resolve(bin, "../../..");
  const dirs = [path.join(usrLib, "x86_64-linux-gnu"), path.join(usrLib, "aarch64-linux-gnu"), path.resolve(bin, "../lib")].filter((dir) => fs.existsSync(dir));
  return dirs.length ? [...dirs, process.env.LD_LIBRARY_PATH].filter(Boolean).join(":") : undefined;
};

export const postgresBin = (() => {
  for (const bin of CANDIDATES) {
    if (!fs.existsSync(path.join(bin, "initdb")) || !fs.existsSync(path.join(bin, "postgres"))) continue;
    const ld = libraryPath(bin);
    const env = { ...process.env, ...(ld ? { LD_LIBRARY_PATH: ld } : {}) };
    const probe = spawnSync(path.join(bin, "postgres"), ["--version"], { env, encoding: "utf8" });
    if (probe.status === 0 && /\b1[6-9]\./.test(probe.stdout)) return { bin, env };
  }
  return undefined;
})();

export interface TestPostgres {
  dir: string;
  socketDir: string;
  port: number;
  connection: { host: string; port: number; database: string; user: string };
  pool(options?: { max?: number }): pg.Pool;
  stop(): Promise<void>;
}

/** `hba` and `ident` replace the generated files (the records service's install policy). */
export const startPostgres = async (options: { hba?: string; ident?: string } = {}): Promise<TestPostgres> => {
  if (!postgresBin) throw new Error("no PostgreSQL binaries");
  const { bin, env } = postgresBin;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-records-pg-"));
  const data = path.join(dir, "data");
  const socketDir = path.join(dir, "sock");
  fs.mkdirSync(socketDir, { mode: 0o700 });
  const init = spawnSync(path.join(bin, "initdb"), ["-D", data, "-U", "postgres", "-A", "trust", "--no-sync", "-E", "UTF8", "--locale=C"], { env, encoding: "utf8" });
  if (init.status !== 0) throw new Error(`initdb failed: ${init.stderr}`);
  if (options.hba !== undefined) fs.writeFileSync(path.join(data, "pg_hba.conf"), options.hba);
  if (options.ident !== undefined) fs.writeFileSync(path.join(data, "pg_ident.conf"), options.ident);
  const port = 20_000 + Math.floor(Math.random() * 30_000);
  const server: ChildProcess = spawn(path.join(bin, "postgres"), [
    "-D", data, "-k", socketDir, "-p", String(port), "-c", "listen_addresses=", "-c", "fsync=off", "-c", "max_connections=60", "-c", "track_commit_timestamp=on",
  ], { env, stdio: ["ignore", "ignore", "pipe"] });
  let log = "";
  server.stderr?.on("data", (chunk: Buffer) => { log = (log + chunk.toString()).slice(-4000); });
  const connection = { host: socketDir, port, database: "postgres", user: "postgres" };
  const deadline = Date.now() + 20_000;
  for (;;) {
    const client = new pg.Client(connection);
    try {
      await client.connect();
      await client.end();
      break;
    } catch (error) {
      await client.end().catch(() => undefined);
      if (Date.now() > deadline || server.exitCode !== null) throw new Error(`postgres did not start: ${String(error)}\n${log}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
  const pools: pg.Pool[] = [];
  return {
    dir, socketDir, port, connection,
    pool: (options = {}) => {
      const pool = new pg.Pool({ ...connection, max: options.max ?? 8 });
      pool.on("error", () => undefined);
      pools.push(pool);
      return pool;
    },
    stop: async () => {
      await Promise.allSettled(pools.map((pool) => pool.end()));
      if (server.exitCode === null) {
        const exited = new Promise((resolve) => server.once("exit", resolve));
        server.kill("SIGINT");
        await Promise.race([exited, new Promise((resolve) => setTimeout(resolve, 10_000))]);
        if (server.exitCode === null) server.kill("SIGKILL");
      }
      fs.rmSync(dir, { recursive: true, force: true });
    },
  };
};
