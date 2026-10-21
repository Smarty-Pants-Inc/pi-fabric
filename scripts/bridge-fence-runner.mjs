#!/usr/bin/env node
// Node (not Bun): exercise the production node:sqlite driver. Jiti only resolves/transpiles
// the checkout's TypeScript; no package build, installed bundle or live mesh is involved.
import { createJiti } from "jiti";
const jiti = createJiti(import.meta.url, { tryNative: false });
await jiti.import("./benchmark-bridge-fence.ts");
