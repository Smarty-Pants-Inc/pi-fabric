// Pi CLI wire substitute for real compiled ResidentHost/launcher integration.
// It delegates to the host, without models or a hand-written custody response.
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import fs from 'node:fs';
const configPath = process.env.PI_FABRIC_RESIDENT_CONFIG;
const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
const release = path.dirname(path.dirname(config.fabricExtensionPath));
const { runResidentHostFromConfigPath } = await import(pathToFileURL(path.join(release, 'dist/residency/host.js')).href);
const controller = new AbortController();
process.on('SIGTERM', () => controller.abort());
process.on('SIGINT', () => controller.abort());
process.stdin.on('end', () => controller.abort()); process.stdin.resume();
try { await runResidentHostFromConfigPath(configPath, controller.signal); }
catch (error) { console.error(error); process.exitCode = 1; }
process.stdin.destroy();
