import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import type {
  AgentTransportAdapter,
  AgentTransportHandle,
  AgentTransportLaunch,
} from "../types.js";
import { EXTERNAL_TRANSPORT_LIVENESS_POLL_INTERVAL_MS } from "../constants.js";
import { scriptSpawnArgs } from "./process-utils.js";

const REQUEST_TIMEOUT_MS = 3_000;
const MAX_RESPONSE_BYTES = 1 * 1024 * 1024;

interface HerdrErrorResponse {
  error?: { code?: string; message?: string };
}

interface HerdrLayoutApplyResponse extends HerdrErrorResponse {
  result?: {
    type?: string;
    layout?: {
      tab_id?: string;
      root?: { type?: string; pane_id?: string };
    };
  };
}

interface HerdrPaneResponse extends HerdrErrorResponse {
  result?: {
    type?: string;
    pane?: { pane_id?: string; terminal_id?: string };
  };
}

// A Herdr error response is definitive. A socket that is missing or refused means the
// server is gone and the request was never delivered. Anything else (timeout, close
// without a reply, reset) is a dropped call whose outcome is unknown: Herdr may have
// applied it (smarty-dev#347).
class HerdrApiError extends Error {
  constructor(message: string, readonly herdrCode: string | undefined) {
    super(message);
  }
}

const serverGone = (error: unknown): boolean => {
  const code = (error as NodeJS.ErrnoException | undefined)?.code;
  return code === "ENOENT" || code === "ECONNREFUSED";
};

const droppedCall = (error: unknown): boolean => !(error instanceof HerdrApiError) && !serverGone(error);

// A live handoff removes the API socket while the panes keep running, so a gone server
// is unknown, not death, until it has been gone this long.
const SERVER_GONE_LIMIT_MS = 5 * 60_000;

const endpointFor = (socketPath: string): string =>
  process.platform === "win32" ? `\\\\.\\pipe\\${socketPath}` : socketPath;

const responseError = (response: HerdrErrorResponse): Error | undefined => {
  if (!response.error) return undefined;
  const code = response.error.code ? `${response.error.code}: ` : "";
  return new HerdrApiError(
    `Herdr API request failed: ${code}${response.error.message ?? "unknown error"}`,
    response.error.code,
  );
};

// The run id tells a person which run a tab belongs to; Fabric never acts on the label.
const runLabel = (request: AgentTransportLaunch): string => `${request.name} · ${request.id.slice(0, 12)}`;

// Every layout.apply spawns a tab, a pty and a worker in the Herdr server. On dev1 an
// actor fan-out drove 260 of them in five minutes and froze Herdr (smarty-dev#266).
const SPAWNS_PER_MINUTE = 20;
// A launch waits for at most this many minute rollovers, then fails.
/** The longest a launch waits for a budget slot, on the monotonic clock. */
const SPAWN_WAIT_MS = 120_000;
// Waiting launches wake across the next minute's first 15 s, not in one burst.
const SPAWN_JITTER_MS = 15_000;

export interface HerdrTransportOptions {
  /** Spawn ledger shared by every Fabric process that launches into one Herdr server. */
  spawnLedgerDir?: string;
  spawnsPerMinute?: number;
  /** Wall clock that names the shared minute slots. */
  now?: () => number;
  /** Monotonic clock for local bounds. */
  monotonicNow?: () => number;
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
  /** Source of the wait jitter, in [0, 1). */
  random?: () => number;
}

export const abortableSleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const timer = setTimeout(() => { signal?.removeEventListener("abort", abort); resolve(); }, ms);
    const abort = (): void => { clearTimeout(timer); reject(signal?.reason); };
    signal?.addEventListener("abort", abort, { once: true });
  });

// The server's stable identity: the socket's real path, following symlinks by hand when a
// live handoff has removed the final target, so an alias and its target always agree.
export const canonicalSocket = (socketPath: string): string => {
  let current = path.resolve(socketPath);
  for (let hops = 0; hops < 40; hops++) {
    try {
      return fs.realpathSync(current);
    } catch {
      try {
        if (!fs.lstatSync(current).isSymbolicLink()) break;
        current = path.resolve(path.dirname(current), fs.readlinkSync(current));
        continue;
      } catch {
        break; // the final target is gone
      }
    }
  }
  try {
    return path.join(fs.realpathSync(path.dirname(current)), path.basename(current));
  } catch {
    return current;
  }
};

// The ledger sits beside the Herdr socket: one per server (its canonical path), in the
// directory of the user who owns that server, and the same for every process whatever
// its TMPDIR. Windows sockets are pipe names, so they hash into the temp directory.
const spawnLedgerFor = (socketPath: string): string => {
  if (process.platform === "win32") {
    const server = createHash("sha256").update(socketPath).digest("hex").slice(0, 16);
    return path.join(os.tmpdir(), "pi-fabric-herdr-spawns", server);
  }
  return `${canonicalSocket(socketPath)}.pi-fabric-spawns`;
};

// Every ancestor must be one that no other account can rename or replace: owned by this
// user or root, and not writable by others unless sticky (as /tmp). Then no pathname used
// below can be redirected after the check.
const unsafeAncestor = (directory: string): string | undefined => {
  const uid = process.getuid?.();
  for (let current = path.dirname(directory); ; current = path.dirname(current)) {
    const stat = fs.lstatSync(current);
    if (stat.isSymbolicLink() || !stat.isDirectory()) return `${current} is not a real directory`;
    if (stat.uid !== uid && stat.uid !== 0) return `${current} is owned by another user`;
    if ((stat.mode & 0o022) !== 0 && (stat.mode & 0o1000) === 0) return `${current} is writable by other users`;
    if (path.dirname(current) === current) return undefined;
  }
};

// Creates the ledger directory, or accepts an existing one, only when it is a real
// directory owned by this user and writable by nobody else, below ancestors that nobody
// else can replace: a slot must not be forgeable or redirected by another account.
const privateLedger = (directory: string): string | undefined => {
  if (process.platform !== "win32") {
    const unsafe = unsafeAncestor(directory);
    if (unsafe) return unsafe;
  }
  try {
    fs.mkdirSync(directory, { mode: 0o700, recursive: process.platform === "win32" });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") return (error as Error).message;
  }
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) return "not a directory";
  if (process.platform === "win32") return undefined;
  if (stat.uid !== process.getuid?.()) return "owned by another user";
  if ((stat.mode & 0o022) !== 0) return "writable by other users";
  return undefined;
};

const warnedLedgers = new Set<string>();
const ledgerUnusable = (directory: string, reason: string): void => {
  if (warnedLedgers.has(directory)) return;
  warnedLedgers.add(directory);
  process.emitWarning(`Herdr launch budget is off: its ledger ${directory} is unusable (${reason})`, {
    code: "PI_FABRIC_HERDR_SPAWN_LEDGER",
  });
};

// Launches in flight in this process, by Herdr server and run id: a repeated launch of
// the same run joins the pending one instead of applying a second layout.
const launching = new Map<string, Promise<AgentTransportHandle>>();

export class HerdrTransport implements AgentTransportAdapter {
  readonly kind = "herdr" as const;

  constructor(
    private readonly environment: NodeJS.ProcessEnv = process.env,
    private readonly options: HerdrTransportOptions = {},
  ) {}

  async available(): Promise<boolean> {
    if (
      this.environment.HERDR_ENV !== "1" ||
      !this.environment.HERDR_SOCKET_PATH ||
      !this.environment.HERDR_WORKSPACE_ID
    ) {
      return false;
    }
    try {
      await this.#request({ method: "ping", params: {} });
      return true;
    } catch {
      return false;
    }
  }

  launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const socket = this.environment.HERDR_SOCKET_PATH ?? "";
    const server = process.platform === "win32" ? socket : canonicalSocket(socket);
    const key = `${server}\0${request.id}`;
    const pending = launching.get(key);
    if (pending) return pending;
    const launched = this.#launch(request).finally(() => {
      if (launching.get(key) === launched) launching.delete(key);
    });
    launching.set(key, launched);
    return launched;
  }

  async #launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const workspaceId = this.environment.HERDR_WORKSPACE_ID;
    if (!workspaceId) throw new Error("Herdr transport requires HERDR_WORKSPACE_ID");
    await this.#claimSpawnSlot(request.signal);
    const label = runLabel(request);
    let paneId: string;
    try {
      paneId = await this.#applyLayout(workspaceId, request, label);
    } catch (error) {
      if (!droppedCall(error)) throw error;
      // Fail closed: layout.apply is not idempotent and a dropped reply leaves its outcome
      // unknown, so Fabric neither adopts a pane nor launches again (smarty-dev#347).
      // ponytail: a pane Herdr created anyway runs unowned; the label names it for cleanup.
      throw new Error(
        `Herdr did not confirm the launch (${(error as Error).message}); a worker may still start in tab "${label}". ` +
        "Fabric does not retry an unconfirmed Herdr launch.",
      );
    }

    let terminalId: string | undefined;
    try {
      const pane = (await this.#request({
        method: "pane.get",
        params: { pane_id: paneId },
      })) as HerdrPaneResponse;
      terminalId = pane.result?.pane?.terminal_id;
    } catch {
      // Very short runs can exit before the optional attach metadata is read.
    }

    const livePane = paneId;
    const monotonicNow = this.options.monotonicNow ?? (() => performance.now());
    let serverGoneSince: number | undefined;
    let lost: string | undefined;
    return {
      kind: this.kind,
      livenessPollIntervalMs: EXTERNAL_TRANSPORT_LIVENESS_POLL_INTERVAL_MS,
      sessionId: livePane,
      // Herdr cannot prove a lost pane's worker is gone (a live handoff keeps it running),
      // so a lost run is never relaunched beside it.
      relaunchable: false,
      ...(terminalId ? { attachCommand: `herdr terminal attach ${terminalId}` } : {}),
      lostContact: () => lost,
      isAlive: async () => {
        try {
          await this.#request({ method: "pane.get", params: { pane_id: livePane } });
          serverGoneSince = undefined;
          lost = undefined;
          return true;
        } catch (error) {
          // Only a reachable server's pane_not_found ends the run at once. A dropped call is
          // re-polled, and a gone server (a live handoff keeps the panes running) ends it
          // only after SERVER_GONE_LIMIT_MS; the run then fails, because Herdr handles are
          // never relaunched, and as lost contact, not an exit (lostContact). ponytail: a
          // Herdr API that stays hung keeps the run alive until its deadline.
          if (error instanceof HerdrApiError && error.herdrCode === "pane_not_found") {
            lost = undefined;
            return false;
          }
          if (!serverGone(error)) return true;
          serverGoneSince ??= monotonicNow();
          if (monotonicNow() - serverGoneSince < SERVER_GONE_LIMIT_MS) return true;
          lost = `the Herdr server has been unreachable for ${Math.round(SERVER_GONE_LIMIT_MS / 1000)} s; pane ${livePane} may still run`;
          return false;
        }
      },
      stop: async () => {
        try {
          await this.#request({ method: "pane.close", params: { pane_id: livePane } });
        } catch {
          // Pane already exited or the owning Herdr server stopped.
        }
      },
    };
  }

  async #applyLayout(workspaceId: string, request: AgentTransportLaunch, label: string): Promise<string> {
    const response = (await this.#request({
      method: "layout.apply",
      params: {
        workspace_id: workspaceId,
        tab_label: label,
        focus: false,
        root: {
          type: "pane",
          label,
          cwd: request.cwd,
          // Herdr starts workers from its server environment, not this Pi host.
          // Forward only the explicit profile selector, never credentials or PATH.
          ...(this.environment.PI_CODING_AGENT_DIR !== undefined
            ? { env: { PI_CODING_AGENT_DIR: this.environment.PI_CODING_AGENT_DIR } }
            : {}),
          command: await scriptSpawnArgs(request.workerPath, request.workerArguments),
        },
      },
    })) as HerdrLayoutApplyResponse;
    const paneId = response.result?.layout?.root?.pane_id;
    if (response.result?.type !== "layout_apply" || !paneId) {
      throw new HerdrApiError("Herdr layout.apply did not return a pane id", undefined);
    }
    return paneId;
  }

  // Takes one of this minute's spawn slots for the Herdr server, waiting for the next
  // minute when all are taken. A slot is a file created exclusively, so every Fabric
  // process of this user shares one budget per server without a lock or a daemon.
  // Fabric builds before this one ignore the ledger: the budget binds only once every
  // process on the host runs a build that has it.
  // ponytail: fixed minute windows allow up to twice the budget across a boundary;
  // a sliding window needs shared state that a file per slot cannot give.
  async #claimSpawnSlot(signal: AbortSignal | undefined): Promise<void> {
    const limit = this.options.spawnsPerMinute ?? SPAWNS_PER_MINUTE;
    const now = this.options.now ?? Date.now;
    const sleep = this.options.sleep ?? abortableSleep;
    const monotonicNow = this.options.monotonicNow ?? (() => performance.now());
    const random = this.options.random ?? Math.random;
    const directory = this.options.spawnLedgerDir ?? spawnLedgerFor(this.environment.HERDR_SOCKET_PATH ?? "");
    // Wall time only names the shared minute slots; the wait itself is bounded monotonically.
    const deadline = monotonicNow() + SPAWN_WAIT_MS;
    const exhausted = (): Error => new Error(
      `Herdr launch budget exhausted: ${limit} launches per minute into this Herdr server, ` +
      `with none free for ${SPAWN_WAIT_MS / 60_000} minutes. Use transport "process" for actors and background agents.`,
    );
    for (;;) {
      if (signal?.aborted) throw signal.reason;
      if (monotonicNow() >= deadline) throw exhausted();
      const minute = Math.floor(now() / 60_000);
      try {
        const unusable = privateLedger(directory);
        if (unusable) {
          ledgerUnusable(directory, unusable);
          return; // an unusable ledger must not stop agents
        }
        for (const entry of fs.readdirSync(directory)) {
          if (Number.parseInt(entry, 10) < minute - 1) fs.rmSync(path.join(directory, entry), { force: true });
        }
        for (let slot = 0; slot < limit; slot++) {
          try {
            fs.closeSync(fs.openSync(path.join(directory, `${minute}-${slot}`), fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY | (fs.constants.O_NOFOLLOW ?? 0), 0o600));
            return;
          } catch (error) {
            if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
          }
        }
      } catch (error) {
        ledgerUnusable(directory, (error as Error).message);
        return;
      }
      const remaining = deadline - monotonicNow();
      if (remaining <= 0) throw exhausted();
      // Clamped, so a wall clock that steps back cannot stretch the wait, and the whole
      // sleep, jitter included, ends by the deadline.
      const untilNextMinute = Math.min(60_000, Math.max(0, (minute + 1) * 60_000 - now()));
      await sleep(Math.min(remaining, untilNextMinute + Math.floor(random() * SPAWN_JITTER_MS)), signal);
    }
  }

  #request(request: { method: string; params: Record<string, unknown> }): Promise<unknown> {
    const socketPath = this.environment.HERDR_SOCKET_PATH;
    if (!socketPath) return Promise.reject(new Error("Herdr transport requires HERDR_SOCKET_PATH"));
    const payload = JSON.stringify({ id: `pi-fabric:${randomUUID()}`, ...request });
    return new Promise((resolve, reject) => {
      const socket = net.createConnection(endpointFor(socketPath));
      const responseChunks: string[] = [];
      let responseBytes = 0;
      let settled = false;
      const finish = (error?: Error, value?: unknown): void => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        socket.destroy();
        if (error) reject(error);
        else resolve(value);
      };
      const timeout = setTimeout(
        () => finish(new Error(`Herdr API request timed out after ${REQUEST_TIMEOUT_MS}ms`)),
        REQUEST_TIMEOUT_MS,
      );
      timeout.unref?.();
      socket.setEncoding("utf8");
      socket.on("connect", () => socket.write(`${payload}\n`));
      socket.on("data", (chunk: string) => {
        const newline = chunk.indexOf("\n");
        const captured = newline < 0 ? chunk : chunk.slice(0, newline);
        responseBytes += Buffer.byteLength(captured, "utf8");
        if (responseBytes > MAX_RESPONSE_BYTES) {
          finish(new Error(`Herdr API response exceeds ${MAX_RESPONSE_BYTES} bytes`));
          return;
        }
        responseChunks.push(captured);
        if (newline < 0) return;
        try {
          const response = JSON.parse(responseChunks.join("")) as HerdrErrorResponse;
          finish(responseError(response), response);
        } catch (error) {
          finish(
            new Error(
              `Invalid Herdr API response: ${error instanceof Error ? error.message : String(error)}`,
            ),
          );
        }
      });
      socket.on("error", (error) => finish(error));
      socket.on("end", () => finish(new Error("Herdr API closed without a response")));
      socket.on("close", () => finish(new Error("Herdr API closed without a response")));
    });
  }
}
