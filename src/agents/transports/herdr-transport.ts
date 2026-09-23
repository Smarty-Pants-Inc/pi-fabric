import net from "node:net";
import { randomUUID } from "node:crypto";
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

interface HerdrTabListResponse extends HerdrErrorResponse {
  result?: { tabs?: Array<{ tab_id?: string; label?: string }> };
}

interface HerdrPaneListResponse extends HerdrErrorResponse {
  result?: { panes?: Array<{ pane_id?: string; tab_id?: string }> };
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

const RECOVERY_ATTEMPTS = 5;
const RECOVERY_DELAY_MS = 200;

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

export class HerdrTransport implements AgentTransportAdapter {
  readonly kind = "herdr" as const;

  constructor(private readonly environment: NodeJS.ProcessEnv = process.env) {}

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

  async launch(request: AgentTransportLaunch): Promise<AgentTransportHandle> {
    const workspaceId = this.environment.HERDR_WORKSPACE_ID;
    if (!workspaceId) throw new Error("Herdr transport requires HERDR_WORKSPACE_ID");
    // layout.apply is not idempotent: remember the existing tabs so a dropped reply can
    // adopt the pane Herdr already created instead of launching a second worker.
    const tabsBefore = await this.#tabs(workspaceId).then(
      (tabs) => new Set(tabs.map((tab) => tab.tab_id)),
      () => undefined,
    );
    let paneId: string | undefined;
    try {
      paneId = await this.#applyLayout(workspaceId, request);
    } catch (error) {
      if (!droppedCall(error) || !tabsBefore) throw error;
      paneId = await this.#adoptLaunchedPane(workspaceId, request.name, tabsBefore);
      if (!paneId) throw error;
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
    return {
      kind: this.kind,
      livenessPollIntervalMs: EXTERNAL_TRANSPORT_LIVENESS_POLL_INTERVAL_MS,
      sessionId: livePane,
      ...(terminalId ? { attachCommand: `herdr terminal attach ${terminalId}` } : {}),
      isAlive: async () => {
        try {
          await this.#request({ method: "pane.get", params: { pane_id: livePane } });
          return true;
        } catch (error) {
          // Only a definitive answer ends the run; a dropped call is re-polled, so a
          // live worker is never stopped and relaunched because one API call was lost.
          // ponytail: a Herdr API that stays hung keeps the run alive until its deadline.
          return !(
            (error instanceof HerdrApiError && error.herdrCode === "pane_not_found") ||
            serverGone(error)
          );
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

  async #applyLayout(workspaceId: string, request: AgentTransportLaunch): Promise<string> {
    const response = (await this.#request({
      method: "layout.apply",
      params: {
        workspace_id: workspaceId,
        tab_label: request.name,
        focus: false,
        root: {
          type: "pane",
          label: request.name,
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

  async #tabs(workspaceId: string): Promise<Array<{ tab_id?: string; label?: string }>> {
    const response = (await this.#request({
      method: "tab.list",
      params: { workspace_id: workspaceId },
    })) as HerdrTabListResponse;
    return response.result?.tabs ?? [];
  }

  // Finds the one tab this launch created (new since `before`, with our label) and
  // returns its pane. Returns undefined when there is none, or more than one, so the
  // caller fails instead of guessing or launching again.
  async #adoptLaunchedPane(
    workspaceId: string,
    label: string,
    before: Set<string | undefined>,
  ): Promise<string | undefined> {
    for (let attempt = 0; attempt < RECOVERY_ATTEMPTS; attempt++) {
      if (attempt > 0) await new Promise((resolve) => setTimeout(resolve, RECOVERY_DELAY_MS));
      try {
        const created = (await this.#tabs(workspaceId)).filter(
          (tab) => tab.label === label && !before.has(tab.tab_id),
        );
        if (created.length > 1) return undefined;
        if (created.length === 0) continue;
        const panes = (await this.#request({
          method: "pane.list",
          params: { workspace_id: workspaceId },
        })) as HerdrPaneListResponse;
        const pane = (panes.result?.panes ?? []).filter((item) => item.tab_id === created[0]!.tab_id);
        return pane.length === 1 ? pane[0]!.pane_id : undefined;
      } catch {
        // Keep trying within the bounded recovery window.
      }
    }
    return undefined;
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
