import fs from "node:fs";
import path from "node:path";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { MainAgentController } from "../../../src/main-agent.js";

/** Real Pi extension, not a recorded/fake session. The reload admission fence is
 * the existing production way to stop between native custody and Pi injection.
 * No control-plane/B2 ACK is manufactured by this fixture. */
export default function mainJournalFixture(pi: ExtensionAPI): void {
  const root = process.env.FABRIC_NATIVE_BOUNDARY_ROOT;
  if (!root) throw new Error("FABRIC_NATIVE_BOUNDARY_ROOT is required");
  let main: MainAgentController | undefined;
  const request = {
    from: { id: "session:boundary-sender", name: "boundary sender", kind: "main" as const },
    verification: "mesh" as const,
    message: "NATIVE_MAIN_JOURNAL_REPLAY_ONCE",
    delivery: "followUp" as const,
    deliveryId: "native-boundary-main-b3",
  };
  pi.on("session_start", (_event, ctx) => {
    const sessionId = ctx.sessionManager.getSessionId();
    main = new MainAgentController(pi, `session:${sessionId}`, true, ctx.cwd, sessionId);
    main.attachFollowUpDrain(ctx, 60_000, path.join(root, "main-followups", `${sessionId}.json`));
    fs.writeFileSync(path.join(root, "main-ready.json"), JSON.stringify({ pid: process.pid, sessionId }));
  });
  pi.registerCommand("native-boundary-admit", {
    description: "Test-only admission at the production reload fence",
    handler: async (_args, ctx) => {
      if (!main) throw new Error("Main not attached");
      main.prepareReload();
      const result = main.deliverAgent(request);
      fs.writeFileSync(path.join(root, "main-admitted.json"), JSON.stringify({
        pid: process.pid, sessionId: ctx.sessionManager.getSessionId(), result,
      }));
    },
  });
  pi.registerCommand("native-boundary-retry", {
    description: "Test-only stable-id retry after native consumption",
    handler: async () => {
      if (!main) throw new Error("Main not attached");
      fs.writeFileSync(path.join(root, "main-retry.json"), JSON.stringify(main.deliverAgent(request)));
    },
  });
  pi.on("session_shutdown", () => main?.closeFollowUpDrain());
}
