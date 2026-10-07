// Runs one resident host from the candidate's own dist/residency/host.js export, exactly as its
// launcher's Pi entry does after spawn (runResidentHostFromConfigPath), without a Pi process.
//   node host.mjs --release DIR --config <residencyRoot>/config.json
import path from 'node:path';
import { argMap, entryModule } from './candidate.mjs';

const args = argMap(process.argv.slice(2));
const { runResidentHostFromConfigPath } = await entryModule(args.release, 'residency/host.js');
try {
  await runResidentHostFromConfigPath(path.resolve(args.config));
  process.exit(0);
} catch (error) {
  process.stderr.write(`resident host failed: ${error?.stack ?? error}\n`);
  process.exit(1);
}
