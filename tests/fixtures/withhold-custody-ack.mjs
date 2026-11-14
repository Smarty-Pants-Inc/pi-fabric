// Test-only race gate: the real worker sends custody, but never receives ACK.
import fs from "node:fs";
import path from "node:path";
if (process.send) {
  const emit = process.emit;
  process.emit = function(event, ...args) {
    if (event === "message" && args[0]?.type === "fabric-execution-custody-ack") {
      fs.writeFileSync(path.join(process.cwd(), "custody-ready.json"), "{}");
      return true;
    }
    return emit.call(this, event, ...args);
  };
}
