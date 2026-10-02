import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { JevFabricServe, JevFabricServeError } from "../../src/jev-fabric/serve.js";

// Loaded by a REAL Pi CLI. No uncaught-exception handler or mocked pipe/backend.
export default function stdinRegression(pi: ExtensionAPI): void {
  pi.registerCommand("sec9-regression", {
    description: "Bounded native-backend stdin failure regression",
    handler: async () => {
      const root = process.env.SEC9_ROOT!;
      const mode = process.env.SEC9_MODE!;
      const connections: JevFabricServe[] = [];
      const open = async (name: string): Promise<JevFabricServe> => {
        const home = path.join(root, name);
        fs.mkdirSync(home, { mode: 0o700 });
        const serve = await JevFabricServe.open(process.env.SEC9_BINARY!, { home, cwd: root, timeoutMs: 10_000 });
        connections.push(serve);
        return serve;
      };
      const result: Record<string, unknown> = { mode, hostPid: process.pid };
      try {
        const broken = await open("broken");
        const healthy = await open("healthy");
        result.phase = "spawn";
        const job = await healthy.request<{ id: string }>("spawn", { argv: ["/bin/cat"], cwd: root, timeoutMs: 10_000 });
        result.phase = "read";
        const read = healthy.request("read", { job: job.id, stream: "stdout", offset: 0, max: 65536, waitMs: 5_000 });
        const pid = Number(fs.readFileSync(path.join(root, "broken", "backend.pid"), "utf8"));
        // Stop the unmodified native backend BEFORE filling its pipe. A 4 MiB
        // write cannot complete in the OS pipe; killing it makes that pending
        // write fail independently of ChildProcess's close event.
        process.kill(pid, "SIGSTOP");
        const outcome = (promise: Promise<unknown>) => promise.then(
          () => ({ unexpectedSuccess: true }),
          (error: unknown) => ({ typed: error instanceof JevFabricServeError,
            name: (error as Error).name, message: (error as Error).message,
            pipeCode: ((error as Error).cause as NodeJS.ErrnoException | undefined)?.code }),
        );
        const first = outcome(broken.request("write", { job: "pending", text: "x".repeat(4 * 1024 * 1024) }));
        const second = outcome(broken.request("status", { job: "pending" }));
        process.kill(pid, "SIGKILL");
        const closing = mode === "shutdown" ? broken.close() : undefined;
        result.failures = await Promise.all([first, second]);
        await closing;
        await broken.close();
        result.backendExited = true;
        result.futureFailure = await outcome(broken.request("status", { job: "pending" }));
        // A different connection's already-pending job completes after the
        // broken pipe, proving failure and teardown stay owner-isolated.
        await healthy.request("write", { job: job.id, text: "unrelated-ok\n" });
        result.unrelatedRead = await read;
        await healthy.request("closeInput", { job: job.id });
        result.unrelatedReceipt = await healthy.request("wait", { job: job.id, timeoutMs: 5_000 });
      } catch (error) {
        result.error = String((error as Error).stack);
      } finally {
        await Promise.all(connections.map(serve => serve.close()));
      }
      fs.writeFileSync(path.join(root, "result.json"), JSON.stringify(result));
    },
  });
}
