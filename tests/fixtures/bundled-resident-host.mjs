// A real child of the launcher-test process. Only execPath is simulated; ppid,
// process birth, /proc exe and the stable host fence remain kernel observations.
import { runResidentHostFromConfigPath } from '../../dist/residency/host.js';
process.execPath = process.env.FABRIC_TEST_BUNDLED_EXEC;
const controller = new AbortController();
process.on('SIGTERM', () => controller.abort());
process.on('SIGINT', () => controller.abort());
try { await runResidentHostFromConfigPath(process.argv[2], controller.signal); }
catch (error) { console.error(error); process.exitCode = 1; }
