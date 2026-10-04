#!/usr/bin/env node
/** Keep native CLI lifecycle/activation behavior while pinning discovery first. */
import path from "node:path";
import { pathToFileURL } from "node:url";
import { installFabricResourcePin } from "./resource-pin.js";
const [sdkDirectory, extensionPath, ...args] = process.argv.slice(2);
if (!sdkDirectory || !extensionPath) throw new Error("Native SDK and pinned Fabric extension are required");
const { DefaultPackageManager } = await import(pathToFileURL(path.join(sdkDirectory, "index.js")).href) as typeof import("@earendil-works/pi-coding-agent");
installFabricResourcePin(DefaultPackageManager, extensionPath);
process.argv = [process.argv[0]!, path.join(sdkDirectory, "cli.js"), ...args];
await import(pathToFileURL(path.join(sdkDirectory, "cli.js")).href);
