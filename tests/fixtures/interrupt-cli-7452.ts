// Deterministic model only. Fabric routing/controller/tools load from the compiled extension.
import fs from "node:fs";
import path from "node:path";
import { fauxAssistantMessage, fauxProvider, fauxToolCall, type Context } from "@earendil-works/pi-ai";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  const root = process.env.FABRIC_7452_GATE_DIR!;
  const mark = (name: string, value: unknown) => fs.writeFileSync(path.join(root, name), JSON.stringify(value));
  let working = false;
  const faux = fauxProvider({ provider: "interrupt-7452", api: "interrupt-7452" });
  pi.registerProvider(faux.provider);
  const reply = (text: string) => (context: Context) => {
    fs.appendFileSync(path.join(root, "provider-context.jsonl"), JSON.stringify(context.messages) + "\n");
    return fauxAssistantMessage(text);
  };
  pi.on("session_start", (_event, ctx) => mark("ready.json", {
    id: `session:${ctx.sessionManager.getSessionId()}`, sessionFile: ctx.sessionManager.getSessionFile(),
  }));
  pi.on("tool_execution_start", (event, ctx) => {
    if (!working || (event.toolName !== "bash" && event.toolName !== "fabric_exec")) return;
    const abort = ctx.abort;
    ctx.abort = () => {
      const startedAt = performance.now();
      abort();
      const pid = Number(fs.readFileSync(path.join(root, "bash-pid"), "utf8"));
      let exited = false;
      while (performance.now() - startedAt < 1000) {
        try { exited = fs.readFileSync(`/proc/${pid}/stat`, "utf8").split(") ")[1]?.startsWith("Z") === true; }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; exited = true; }
        if (exited) break;
      }
      mark("abort.json", { nativeAbortToProcessExitMs: performance.now() - startedAt, exited });
    };
  });
  pi.on("input", event => {
    if (event.text.startsWith("WORK ")) {
      working = true;
      const command = `echo $$ > '${root}/bash-pid'; printf entered > '${root}/entered'; while ! test -f '${root}/release'; do sleep 0.01; done; printf completed > '${root}/completed'`;
      const tool = event.text.slice(5) === "fabric_exec"
        ? fauxToolCall("fabric_exec", { code: "return await pi.bash({ command: π.command, timeout: 30 });", payloads: { command } })
        : fauxToolCall("bash", { command, timeout: 30 });
      faux.setResponses([fauxAssistantMessage(tool, { stopReason: "toolUse" }), reply("HOLD received, no retry")]);
    }
    if (event.text.startsWith("SEND ")) {
      const packet = event.text.slice(5);
      faux.setResponses([
        fauxAssistantMessage(fauxToolCall("fabric_exec", { code: "return await agents.send(JSON.parse(π.packet));", payloads: { packet } }), { stopReason: "toolUse" }),
        reply("interrupt acknowledged"),
      ]);
    }
    return { action: "continue" as const };
  });
}
