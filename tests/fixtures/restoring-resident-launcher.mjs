// Real resident restoration and real workers, with readiness withheld to model
// a startup hanging after restored execution has begun. No fake manager/response.
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
const configPath = process.argv[process.argv.indexOf("--config") + 1];
const config = JSON.parse(fs.readFileSync(configPath, "utf8"));
const { ResidentHost } = await import(pathToFileURL(path.join(path.dirname(config.fabricExtensionPath), "residency/host.js")).href);
const rename = fs.renameSync;
fs.renameSync = (from, to) => rename(from, String(to) === path.join(config.residencyRoot, "maintenance-ready.json") ? `${to}.withheld` : to);
fs.appendFileSync(path.join(config.residencyRoot, "launcher.log"), JSON.stringify({ event: "launcher-started", pid: process.pid }) + "\n");
const host = new ResidentHost(config);
await host.start().catch(error => { fs.writeFileSync(path.join(config.residencyRoot, "restore-error.json"), String(error.stack)); throw error; });
fs.writeFileSync(path.join(config.residencyRoot, "restore-state.json"), JSON.stringify(host.actors.list()));
process.on("SIGTERM", () => { void host.close().then(() => process.exit(0)); });
