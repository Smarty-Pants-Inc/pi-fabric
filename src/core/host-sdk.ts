import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

type PiSdk = typeof import("@earendil-works/pi-coding-agent");

/**
 * The SDK entry of the bundled Pi CLI running this process, if it is one. The
 * published `pi` bin is `dist/bundle/cli.js`; its `dist/bundle/index.js` SDK
 * entry is built from the same chunks the CLI has already evaluated.
 */
export function hostBundleSdkEntry(cliPath: string | undefined = process.argv[1]): string | undefined {
  if (!cliPath) return undefined;
  let cli: string;
  try {
    cli = fs.realpathSync(cliPath);
  } catch {
    return undefined;
  }
  const bundle = path.dirname(cli);
  if (path.basename(cli) !== "cli.js" || path.basename(bundle) !== "bundle") return undefined;
  const entry = path.join(bundle, "index.js");
  try {
    const manifest = JSON.parse(fs.readFileSync(path.join(bundle, "..", "..", "package.json"), "utf8")) as { name?: unknown };
    return manifest.name === "@earendil-works/pi-coding-agent" && fs.existsSync(entry) ? entry : undefined;
  } catch {
    return undefined;
  }
}

/**
 * The host's own SDK. Pi imports Fabric natively (it is ESM JavaScript), so
 * jiti's virtual modules do not apply, and a bare package import evaluates the
 * separate unbundled SDK inside the bundled CLI: about 1,600 more modules in an
 * `extensions: false` child that loads only the provider bridge. Besides
 * the startup cost, that CLI enables Node's compile cache, which serializes
 * every one of those modules on exit; with a cold cache that held a Windows
 * child past the worker's exit grace after it had settled. The bundle's SDK
 * entry shares the CLI's already loaded chunks instead. Unbundled CLIs, the
 * durable worker, and embeds keep the package entry.
 */
export async function loadHostSdk(cliPath: string | undefined = process.argv[1]): Promise<PiSdk> {
  const entry = hostBundleSdkEntry(cliPath);
  if (entry) {
    try {
      return await import(pathToFileURL(entry).href) as PiSdk;
    } catch {
      // Unexpected bundle layout: fall back to the package entry.
    }
  }
  return import("@earendil-works/pi-coding-agent");
}
