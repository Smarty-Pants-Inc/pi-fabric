// Deliberately publish a terminal result before the root writer exits.
import "./fake-worker.mjs";
await new Promise(resolve => setTimeout(resolve, 1000));
