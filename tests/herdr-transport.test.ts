import fs from "node:fs";
import net, { type Server } from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { HerdrTransport } from "../src/agents/transports/herdr-transport.js";

const servers: Server[] = [];
const roots: string[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          server.close(() => resolve());
        }),
    ),
  );
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

type Drop = "apply-after-create" | "apply-before-create" | "apply-error" | "pane-get";

const startServer = async (options: { drop?: Drop } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-"));
  roots.push(root);
  const socketPath = path.join(root, "herdr.sock");
  let paneAlive = true;
  const tabs = [{ tab_id: "w1:t1", label: "Main" }];
  const panes = [{ pane_id: "w1:p1", tab_id: "w1:t1" }];
  const requests: Array<{ method: string; params: Record<string, unknown> }> = [];
  const server = net.createServer((socket) => {
    let input = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => {
      input += chunk;
      const newline = input.indexOf("\n");
      if (newline < 0) return;
      const request = JSON.parse(input.slice(0, newline)) as {
        id: string;
        method: string;
        params: Record<string, unknown>;
      };
      requests.push({ method: request.method, params: request.params });
      let response: unknown;
      // A dropped call: Herdr may or may not have applied it, but no reply arrives.
      const drop = (): void => { socket.destroy(); };
      if (request.method === "layout.apply" && options.drop === "apply-before-create") return drop();
      if (request.method === "layout.apply" && options.drop === "apply-error") {
        socket.end(`${JSON.stringify({ id: request.id, error: { code: "invalid_layout", message: "bad" } })}\n`);
        return;
      }
      if (request.method === "layout.apply") {
        tabs.push({ tab_id: "w1:t2", label: String(request.params.tab_label) });
        panes.push({ pane_id: "w1:p2", tab_id: "w1:t2" });
        if (options.drop === "apply-after-create") return drop();
      }
      if (request.method === "pane.get" && options.drop === "pane-get") return drop();
      if (request.method === "ping") {
        response = { id: request.id, result: { type: "pong", version: "test", protocol: 17 } };
      } else if (request.method === "layout.apply") {
        response = {
          id: request.id,
          result: {
            type: "layout_apply",
            layout: {
              workspace_id: "w1",
              tab_id: "w1:t2",
              zoomed: false,
              focused_pane_id: "w1:p2",
              root: { type: "pane", pane_id: "w1:p2" },
            },
          },
        };
      } else if (request.method === "pane.get" && paneAlive) {
        response = {
          id: request.id,
          result: {
            type: "pane_info",
            pane: { pane_id: "w1:p2", terminal_id: "term_worker" },
          },
        };
      } else if (request.method === "tab.list") {
        response = { id: request.id, result: { type: "tab_list", tabs } };
      } else if (request.method === "pane.list") {
        response = { id: request.id, result: { type: "pane_list", panes } };
      } else if (request.method === "pane.close" && paneAlive) {
        paneAlive = false;
        response = { id: request.id, result: { type: "ok" } };
      } else {
        response = { id: request.id, error: { code: "pane_not_found", message: "pane not found" } };
      }
      socket.end(`${JSON.stringify(response)}\n`);
    });
  });
  servers.push(server);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, resolve);
  });
  return { socketPath, requests, server };
};

const env = (socketPath: string) => ({ HERDR_ENV: "1", HERDR_SOCKET_PATH: socketPath, HERDR_WORKSPACE_ID: "w1" });
const launchRequest = { id: "run-1", name: "review worker", cwd: "/repo", workerPath: "/fabric/worker.js", workerArguments: [] };

describe.skipIf(process.platform === "win32")("HerdrTransport", () => {
  it("is available only inside a reachable Herdr workspace", async () => {
    const { socketPath } = await startServer();
    const available = new HerdrTransport({
      HERDR_ENV: "1",
      HERDR_SOCKET_PATH: socketPath,
      HERDR_WORKSPACE_ID: "w1",
    });
    await expect(available.available()).resolves.toBe(true);
    await expect(new HerdrTransport({ HERDR_ENV: "1" }).available()).resolves.toBe(false);
  });

  it.each([undefined, "", "/profiles/custom agent 'quoted'"])(
    "forwards only an explicitly selected Pi profile (%j)",
    async (profile) => {
      const { socketPath, requests } = await startServer();
      const transport = new HerdrTransport({
        HERDR_ENV: "1",
        HERDR_SOCKET_PATH: socketPath,
        HERDR_WORKSPACE_ID: "w1",
        ...(profile === undefined ? {} : { PI_CODING_AGENT_DIR: profile }),
        PATH: "/parent/private/bin",
        OPENAI_API_KEY: "must-not-forward",
        OP_SERVICE_ACCOUNT_TOKEN: "must-not-forward",
      });
      await transport.launch({
        id: "profile-probe", name: "profile probe", cwd: "/repo",
        workerPath: "/fabric/worker.js", workerArguments: [],
      });
      const root = requests.find((request) => request.method === "layout.apply")
        ?.params.root as Record<string, unknown>;
      if (profile === undefined) expect(root).not.toHaveProperty("env");
      else expect(root.env).toEqual({ PI_CODING_AGENT_DIR: profile });
      expect(root.command).toEqual([process.execPath, "/fabric/worker.js"]);
    },
  );

  it("launches an argv-backed background tab and controls it by pane id", async () => {
    const { socketPath, requests } = await startServer();
    const transport = new HerdrTransport({
      HERDR_ENV: "1",
      HERDR_SOCKET_PATH: socketPath,
      HERDR_WORKSPACE_ID: "w1",
    });
    const handle = await transport.launch({
      id: "agent-id",
      name: "review worker",
      cwd: "/repo with spaces",
      workerPath: "/fabric/worker.js",
      workerArguments: ["--task-file", "/tmp/task with spaces.txt"],
    });

    expect(handle).toMatchObject({
      kind: "herdr",
      sessionId: "w1:p2",
      attachCommand: "herdr terminal attach term_worker",
    });
    const apply = requests.find((request) => request.method === "layout.apply");
    expect(apply?.params).toEqual({
      workspace_id: "w1",
      tab_label: "review worker",
      focus: false,
      root: {
        type: "pane",
        label: "review worker",
        cwd: "/repo with spaces",
        command: [
          process.execPath,
          "/fabric/worker.js",
          "--task-file",
          "/tmp/task with spaces.txt",
        ],
      },
    });
    await expect(handle.isAlive()).resolves.toBe(true);
    await handle.stop();
    await expect(handle.isAlive()).resolves.toBe(false);
  });

  // smarty-dev#347: a dropped Herdr call must never respawn or duplicate a live worker.
  it("adopts the pane when the layout.apply reply is dropped after Herdr created it", async () => {
    const { socketPath, requests } = await startServer({ drop: "apply-after-create" });
    const handle = await new HerdrTransport(env(socketPath)).launch(launchRequest);
    expect(handle.sessionId).toBe("w1:p2");
    expect(requests.filter((request) => request.method === "layout.apply")).toHaveLength(1);
  });

  it("fails without a second launch when the dropped layout.apply created nothing", async () => {
    const { socketPath, requests } = await startServer({ drop: "apply-before-create" });
    await expect(new HerdrTransport(env(socketPath)).launch(launchRequest)).rejects.toThrow("closed without a response");
    expect(requests.filter((request) => request.method === "layout.apply")).toHaveLength(1);
  });

  it("does not try to recover from a definitive Herdr error", async () => {
    const { socketPath, requests } = await startServer({ drop: "apply-error" });
    await expect(new HerdrTransport(env(socketPath)).launch(launchRequest)).rejects.toThrow("invalid_layout");
    expect(requests.filter((request) => request.method === "tab.list")).toHaveLength(1); // only the pre-launch snapshot
  });

  it("keeps a worker alive when a liveness call is dropped, and ends it only when Herdr says so", async () => {
    const dropped = await startServer({ drop: "pane-get" });
    const handle = await new HerdrTransport(env(dropped.socketPath)).launch(launchRequest);
    await expect(handle.isAlive()).resolves.toBe(true);

    const healthy = await startServer();
    const live = await new HerdrTransport(env(healthy.socketPath)).launch(launchRequest);
    await live.stop();
    await expect(live.isAlive()).resolves.toBe(false);            // pane_not_found

    await new Promise<void>((resolve) => healthy.server.close(() => resolve()));
    fs.rmSync(healthy.socketPath, { force: true });
    await expect(live.isAlive()).resolves.toBe(false);            // the Herdr server is gone
  });
});
