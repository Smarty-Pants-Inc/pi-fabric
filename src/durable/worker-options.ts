import path from "node:path";
import type { ThinkingLevel } from "@earendil-works/pi-agent-core";

export interface DurableWorkerOptions {
  directory: string;
  runId: string;
  sessionFile?: string;
  model?: string;
  provider?: string;
  thinking?: ThinkingLevel;
  tools?: string[];
  noTools?: "all";
  extensions: string[];
  noExtensions: boolean;
  appendSystemPrompt: string;
}

/** Internal worker protocol, deliberately not a second general-purpose Pi CLI. */
export function parseDurableWorkerOptions(argv: readonly string[], cwd = process.cwd()): DurableWorkerOptions {
  const options: DurableWorkerOptions = {
    directory: "", runId: "", extensions: [], noExtensions: false, appendSystemPrompt: "",
  };
  const prompts: string[] = [];
  for (let i = 0; i < argv.length; i++) {
    const flag = argv[i]!;
    if (flag === "--no-session") continue;
    if (flag === "--no-extensions") { options.noExtensions = true; continue; }
    if (flag === "--no-tools") { options.noTools = "all"; continue; }
    if (!["--mode", "--durable-directory", "--durable-run-id", "--session", "--model", "--provider", "--thinking", "--tools", "-e", "--extension", "--append-system-prompt"].includes(flag)) {
      throw new Error(`Unsupported durable worker option: ${flag}`);
    }
    const value = argv[++i];
    if (value === undefined || value.startsWith("--")) throw new Error(`Missing value for ${flag}`);
    switch (flag) {
      case "--mode": if (value !== "rpc") throw new Error("The durable worker only supports RPC mode"); break;
      case "--durable-directory": options.directory = path.resolve(cwd, value); break;
      case "--durable-run-id": options.runId = value.trim(); break;
      case "--session": options.sessionFile = path.resolve(cwd, value); break;
      case "--model": options.model = value; break;
      case "--provider": options.provider = value; break;
      case "--thinking":
        if (!["off", "minimal", "low", "medium", "high", "xhigh", "max"].includes(value)) throw new Error(`Invalid thinking level: ${value}`);
        options.thinking = value as ThinkingLevel;
        break;
      case "--tools": options.tools = value.split(",").map(tool => tool.trim()).filter(Boolean); break;
      case "-e": case "--extension": options.extensions.push(value.startsWith("builtin:") ? value : path.resolve(cwd, value)); break;
      case "--append-system-prompt": prompts.push(value); break;
    }
  }
  if (!options.directory || !options.runId) throw new Error("Durable workers require --durable-directory and --durable-run-id");
  if (options.tools && options.noTools) throw new Error("--tools and --no-tools are mutually exclusive");
  options.appendSystemPrompt = prompts.join("\n\n");
  return options;
}
