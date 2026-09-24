import fs from "node:fs";
import net, { type Server } from "node:net";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { HerdrTransport, abortableSleep, canonicalSocket, type HerdrTransportOptions } from "../src/agents/transports/herdr-transport.js";

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

const startServer = async (options: { drop?: Drop; tabs?: Array<{ tab_id: string; label: string }> } = {}) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-"));
  roots.push(root);
  const socketPath = path.join(root, "herdr.sock");
  let paneAlive = true;
  const tabs = [{ tab_id: "w1:t1", label: "Main" }, ...(options.tabs ?? [])];
  const panes = tabs.map((tab) => ({ pane_id: tab.tab_id.replace(":t", ":p"), tab_id: tab.tab_id }));
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
      } else if (request.method === "tab.close") {
        const index = tabs.findIndex((tab) => tab.tab_id === request.params.tab_id);
        if (index >= 0) tabs.splice(index, 1);
        response = index >= 0
          ? { id: request.id, result: { type: "ok" } }
          : { id: request.id, error: { code: "tab_not_found", message: "tab not found" } };
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
// Each test keeps its spawn ledger beside its socket, inside the temporary root.
const ledger = (socketPath: string) => path.join(path.dirname(socketPath), "spawns");
const herdr = (socketPath: string, options: HerdrTransportOptions = {}) =>
  new HerdrTransport(env(socketPath), { spawnLedgerDir: ledger(socketPath), ...options });
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
    const transport = herdr(socketPath);
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
      tab_label: "review worker · agent-id",
      focus: false,
      root: {
        type: "pane",
        label: "review worker · agent-id",
        cwd: "/repo with spaces",
        command: [
          process.execPath,
          "/fabric/worker.js",
          "--task-file",
          "/tmp/task with spaces.txt",
        ],
      },
    });
    expect(handle.relaunchable).toBe(false);
    await expect(handle.isAlive()).resolves.toBe(true);
    await handle.stop();
    await expect(handle.isAlive()).resolves.toBe(false);
  });

  // smarty-dev#347: a dropped Herdr call must never respawn or duplicate a live worker.
  it("fails closed, without adopting or launching again, when a layout.apply reply is dropped", async () => {
    for (const drop of ["apply-after-create", "apply-before-create"] as const) {
      const { socketPath, requests } = await startServer({ drop });
      await expect(herdr(socketPath).launch(launchRequest)).rejects.toThrow(
        'a worker may still start in tab "review worker · run-1". Fabric does not retry an unconfirmed Herdr launch',
      );
      expect(requests.map((request) => request.method)).toEqual(["layout.apply"]);   // no listing, adoption or retry
    }
  });

  it("does not try to recover from a definitive Herdr error", async () => {
    const { socketPath, requests } = await startServer({ drop: "apply-error" });
    await expect(herdr(socketPath).launch(launchRequest)).rejects.toThrow("invalid_layout");
    expect(requests.map((request) => request.method)).toEqual(["layout.apply"]);
  });

  it("keeps a worker alive when a liveness call is dropped, and ends it only when Herdr says so", async () => {
    const dropped = await startServer({ drop: "pane-get" });
    const handle = await herdr(dropped.socketPath).launch(launchRequest);
    await expect(handle.isAlive()).resolves.toBe(true);

    const healthy = await startServer();
    const live = await herdr(healthy.socketPath).launch(launchRequest);
    await live.stop();
    await expect(live.isAlive()).resolves.toBe(false);            // pane_not_found from a reachable server
  });

  // dev-lead review F1: a live handoff removes the socket while the panes keep running.
  it("treats a gone Herdr server as a live handoff until it stays gone for five minutes", async () => {
    const { socketPath, server } = await startServer();
    let monotonic = 0;
    const handle = await herdr(socketPath, { monotonicNow: () => monotonic }).launch(launchRequest);
    await new Promise<void>((resolve) => server.close(() => resolve()));
    fs.rmSync(socketPath, { force: true });
    await expect(handle.isAlive()).resolves.toBe(true);           // gone at t=0: unknown
    monotonic = 4 * 60_000;
    await expect(handle.isAlive()).resolves.toBe(true);
    expect(handle.lostContact?.()).toBeUndefined();
    monotonic = 5 * 60_000;
    await expect(handle.isAlive()).resolves.toBe(false);          // gone for the whole bound: lost, not proven dead
    expect(handle.lostContact?.()).toMatch(/Herdr server has been unreachable for 300 s; pane .* may still run/);
  });

  it("does not report lost contact for a pane that a reachable server says is gone", async () => {
    const { socketPath } = await startServer();
    const handle = await herdr(socketPath).launch(launchRequest);
    await handle.stop();
    await expect(handle.isAlive()).resolves.toBe(false);
    expect(handle.lostContact?.()).toBeUndefined();
  });

  // dev-lead review F3/F5: adoption and cleanup match this run's id, never a shared name.
  // smarty-dev#266: 2,264 layout.apply calls from fleet actors froze dev1's Herdr.
  describe("layout.apply budget", () => {
    const applies = (requests: Array<{ method: string }>) =>
      requests.filter((request) => request.method === "layout.apply").length;
    // A sleep advances the monotonic clock by ms and the wall clock by ms + wallDrift (a
    // negative drift is a wall clock that steps back during each wait).
    const clock = (start: number, wallDrift = 0) => {
      const state = { now: start, monotonic: 0, sleeps: [] as number[] };
      return {
        state,
        now: () => state.now,
        monotonicNow: () => state.monotonic,
        sleep: async (ms: number, signal?: AbortSignal) => {
          if (signal?.aborted) throw signal.reason;
          state.sleeps.push(ms);
          state.now += ms + wallDrift;
          state.monotonic += ms;
        },
      };
    };

    it("joins a repeated launch of the same run to the pending one", async () => {
      const { socketPath, requests } = await startServer();
      const transport = herdr(socketPath);
      const [first, second] = await Promise.all([transport.launch(launchRequest), transport.launch(launchRequest)]);
      expect(second).toBe(first);
      expect(applies(requests)).toBe(1);
    });

    it("shares one per-minute budget across processes and waits for the next minute", async () => {
      const { socketPath, requests } = await startServer();
      const time = clock(1_000 * 60_000 + 5_000);
      // A transport per launch stands in for separate Fabric processes: only the ledger is shared.
      const launch = (id: string) =>
        herdr(socketPath, { spawnsPerMinute: 2, now: time.now, monotonicNow: time.monotonicNow, sleep: time.sleep }).launch({ ...launchRequest, id });
      await launch("run-a");
      await launch("run-b");
      expect(time.state.sleeps).toEqual([]);
      await launch("run-c");
      expect(applies(requests)).toBe(3);
      expect(time.state.sleeps).toHaveLength(1);
      expect(time.state.sleeps[0]).toBeGreaterThanOrEqual(55_000);
      expect(Math.floor(time.state.now / 60_000)).toBe(1_001);
    });

    it("fails without applying when no slot frees within the wait limit", async () => {
      const { socketPath, requests } = await startServer();
      const time = clock(2_000 * 60_000);
      fs.mkdirSync(ledger(socketPath), { recursive: true, mode: 0o700 });
      for (let minute = 2_000; minute <= 2_003; minute++) fs.writeFileSync(path.join(ledger(socketPath), `${minute}-0`), "");
      const transport = herdr(socketPath, { spawnsPerMinute: 1, now: time.now, monotonicNow: time.monotonicNow, sleep: time.sleep });
      await expect(transport.launch(launchRequest)).rejects.toThrow("Herdr launch budget exhausted");
      expect(applies(requests)).toBe(0);
    });

    // dev-lead review D6: the two-minute bound is monotonic, jitter included.
    it("fails at the monotonic deadline instead of applying late, even with maximum jitter", async () => {
      const { socketPath, requests } = await startServer();
      const time = clock(2_100 * 60_000);                        // a full minute boundary
      fs.mkdirSync(ledger(socketPath), { recursive: true, mode: 0o700 });
      for (const minute of [2_100, 2_101]) fs.writeFileSync(path.join(ledger(socketPath), `${minute}-0`), "");
      // Minute 2_102 is free: the old two-rollover wait reached it at 134 s and applied late.
      const transport = herdr(socketPath, { spawnsPerMinute: 1, now: time.now, monotonicNow: time.monotonicNow, sleep: time.sleep, random: () => 0.999 });
      await expect(transport.launch(launchRequest)).rejects.toThrow("Herdr launch budget exhausted");
      expect(applies(requests)).toBe(0);
      expect(time.state.monotonic).toBeLessThanOrEqual(120_000);
    });

    it("keeps the monotonic bound when the wall clock steps back during each wait", async () => {
      const { socketPath, requests } = await startServer();
      const time = clock(2_200 * 60_000, -50_000);
      fs.mkdirSync(ledger(socketPath), { recursive: true, mode: 0o700 });
      for (let minute = 2_195; minute <= 2_205; minute++) fs.writeFileSync(path.join(ledger(socketPath), `${minute}-0`), "");
      const transport = herdr(socketPath, { spawnsPerMinute: 1, now: time.now, monotonicNow: time.monotonicNow, sleep: time.sleep, random: () => 0.999 });
      await expect(transport.launch(launchRequest)).rejects.toThrow("Herdr launch budget exhausted");
      expect(applies(requests)).toBe(0);
      expect(time.state.monotonic).toBeLessThanOrEqual(120_000);
    });

    it("stops waiting when the manager closes", async () => {
      const { socketPath, requests } = await startServer();
      const time = clock(3_000 * 60_000);
      fs.mkdirSync(ledger(socketPath), { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(ledger(socketPath), "3000-0"), "");
      const closing = new AbortController();
      const transport = herdr(socketPath, {
        spawnsPerMinute: 1,
        now: time.now,
        monotonicNow: time.monotonicNow,
        sleep: async (ms, signal) => { closing.abort(new Error("Fabric agent manager is closing")); await time.sleep(ms, signal); },
      });
      await expect(transport.launch({ ...launchRequest, signal: closing.signal })).rejects.toThrow("closing");
      expect(applies(requests)).toBe(0);
    });

    it("launches without the budget, and warns once, when the ledger is unusable", async () => {
      const { socketPath, requests } = await startServer();
      const blocked = path.join(path.dirname(socketPath), "not-a-directory");
      fs.writeFileSync(blocked, "");
      const warnings: string[] = [];
      const onWarning = (warning: Error & { code?: string }) => { if (warning.code === "PI_FABRIC_HERDR_SPAWN_LEDGER") warnings.push(warning.message); };
      process.on("warning", onWarning);
      try {
        await herdr(socketPath, { spawnLedgerDir: blocked }).launch(launchRequest);
        await herdr(socketPath, { spawnLedgerDir: blocked }).launch({ ...launchRequest, id: "run-2" });
        await new Promise((resolve) => setImmediate(resolve));
      } finally {
        process.off("warning", onWarning);
      }
      expect(applies(requests)).toBe(2);
      expect(warnings).toEqual([expect.stringContaining("not a directory")]);
    });

    // dev-lead review N1/N2: one private ledger per server, beside its socket.
    it.skipIf(process.platform === "win32")("keeps the ledger private and beside the server's real socket", async () => {
      const { socketPath } = await startServer();
      const linkDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-link-"));
      roots.push(linkDir);
      const linked = path.join(linkDir, "herdr.sock");
      fs.symlinkSync(socketPath, linked);
      await new HerdrTransport(env(linked)).launch(launchRequest);
      const ledgerDir = `${socketPath}.pi-fabric-spawns`;
      expect(fs.statSync(ledgerDir).mode & 0o777).toBe(0o700);
      expect(fs.readdirSync(ledgerDir)).toHaveLength(1);
      expect(fs.existsSync(`${linked}.pi-fabric-spawns`)).toBe(false);
    });

    it.skipIf(process.platform === "win32")("does not use a ledger that another path redirects", async () => {
      const { socketPath, requests } = await startServer();
      const elsewhere = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-elsewhere-"));
      roots.push(elsewhere);
      fs.symlinkSync(elsewhere, `${socketPath}.pi-fabric-spawns`);
      await new HerdrTransport(env(socketPath)).launch(launchRequest);
      expect(applies(requests)).toBe(1);
      expect(fs.readdirSync(elsewhere)).toEqual([]);                // nothing written through the link
    });

    it.skipIf(process.platform === "win32")("does not use a ledger that other users can write", async () => {
      const { socketPath, requests } = await startServer();
      const shared = `${socketPath}.pi-fabric-spawns`;
      fs.mkdirSync(shared);
      fs.chmodSync(shared, 0o777);
      await new HerdrTransport(env(socketPath)).launch(launchRequest);
      expect(applies(requests)).toBe(1);
      expect(fs.readdirSync(shared)).toEqual([]);
    });

    // dev-lead review D4: a private parent is not enough when a higher ancestor can replace it.
    it.skipIf(process.platform === "win32")("does not use a ledger below an ancestor that others can replace", async () => {
      const { socketPath, requests } = await startServer();
      const shared = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-shared-"));
      roots.push(shared);
      const session = path.join(shared, "session");
      fs.mkdirSync(session, { mode: 0o700 });
      fs.chmodSync(shared, 0o777);                               // other-writable, not sticky
      await herdr(socketPath, { spawnLedgerDir: path.join(session, "spawns") }).launch(launchRequest);
      expect(applies(requests)).toBe(1);
      expect(fs.existsSync(path.join(session, "spawns"))).toBe(false);
    });

    it.skipIf(process.platform === "win32")("accepts a ledger below a sticky shared ancestor", async () => {
      const { socketPath } = await startServer();
      const shared = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-sticky-"));
      roots.push(shared);
      const session = path.join(shared, "session");
      fs.mkdirSync(session, { mode: 0o700 });
      fs.chmodSync(shared, 0o1777);                              // as /tmp
      await herdr(socketPath, { spawnLedgerDir: path.join(session, "spawns") }).launch(launchRequest);
      expect(fs.readdirSync(path.join(session, "spawns"))).toHaveLength(1);
    });

    // dev-lead review D5: an alias whose target a live handoff removed keeps the server's identity.
    it.skipIf(process.platform === "win32")("gives an alias the same identity while its target is gone and after it returns", () => {
      const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-alias-")));
      roots.push(dir);
      const target = path.join(dir, "herdr.sock");
      const alias = path.join(dir, "alias.sock");
      const chained = path.join(dir, "chained.sock");
      fs.symlinkSync(target, alias);
      fs.symlinkSync(alias, chained);
      expect(canonicalSocket(alias)).toBe(target);               // target missing
      expect(canonicalSocket(chained)).toBe(target);
      fs.writeFileSync(target, "");
      expect(canonicalSocket(alias)).toBe(target);               // target back
      expect(canonicalSocket(chained)).toBe(target);
      fs.rmSync(target);
      expect(canonicalSocket(target)).toBe(target);
    });

    it.skipIf(process.platform === "win32")("joins launches of one run through an alias and its target", async () => {
      const { socketPath, requests } = await startServer();
      const linkDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-link-"));
      roots.push(linkDir);
      const linked = path.join(linkDir, "herdr.sock");
      fs.symlinkSync(socketPath, linked);
      const [first, second] = await Promise.all([
        new HerdrTransport(env(socketPath)).launch(launchRequest),
        new HerdrTransport(env(linked)).launch(launchRequest),
      ]);
      expect(second).toBe(first);
      expect(applies(requests)).toBe(1);
    });

    it.skipIf(process.platform === "win32")("does not use a ledger whose parent directory others can write", async () => {
      const { socketPath, requests } = await startServer();
      const shared = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-herdr-shared-"));
      roots.push(shared);
      fs.chmodSync(shared, 0o777);
      await herdr(socketPath, { spawnLedgerDir: path.join(shared, "spawns") }).launch(launchRequest);
      expect(applies(requests)).toBe(1);
      expect(fs.existsSync(path.join(shared, "spawns"))).toBe(false);
    });

    it("ends a real wait at once when the signal aborts", async () => {
      const closing = new AbortController();
      const started = performance.now();
      const waiting = abortableSleep(60_000, closing.signal);
      setTimeout(() => closing.abort(new Error("Fabric agent manager is closing")), 20);
      await expect(waiting).rejects.toThrow("closing");
      expect(performance.now() - started).toBeLessThan(1_000);
    });
  });
});
