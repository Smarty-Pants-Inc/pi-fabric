// Fault the real compiled host's first required receipt, not its business methods.
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
const fault = process.env.PI_FABRIC_TEST_READINESS_FAULT;
if (fault) {
  const rename = fs.renameSync;
  fs.renameSync = (source, target) => {
    if (String(target) === process.env.PI_FABRIC_TEST_READINESS_PATH && fs.existsSync(fault)) {
      fs.writeFileSync(`${fault}.entered`, JSON.stringify({ pid: process.pid, at: Date.now() }));
      if (process.env.PI_FABRIC_TEST_READINESS_MODE === "held") {
        // A real synchronous publication stuck past the originating client's budget.
        // No mocked client/launcher/host execution or synthetic readiness result.
        while (fs.existsSync(fault)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
      } else {
        throw new Error("injected first readiness publication failure");
      }
    }
    return rename(source, target);
  };
  syncBuiltinESMExports();
}
