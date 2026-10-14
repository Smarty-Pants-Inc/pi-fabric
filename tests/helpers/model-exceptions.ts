import fs from "node:fs";
import path from "node:path";

/** Provision only a caller-owned private fixture directory; never change host config. */
export const writeModelExceptions = (agentDir: string, models: string[]): string => {
  const filePath = path.join(agentDir, "model-exceptions.json");
  fs.writeFileSync(filePath, JSON.stringify([{
    ref: "smarty-dev#11864",
    models,
    expires: "2999-01-01T00:00:00.000Z",
  }]), { mode: 0o600 });
  return filePath;
};
