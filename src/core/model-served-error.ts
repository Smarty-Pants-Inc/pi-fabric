/** A process task's requested model is absent from an authoritative provider catalog. */
export class FabricModelNotServedError extends Error {
  readonly code = "FABRIC_MODEL_NOT_SERVED";
  readonly provider: string;
  readonly model: string;

  constructor(selector: string, fallbackProvider = "unknown", detail?: string) {
    const separator = selector.indexOf("/");
    const provider = separator > 0 ? selector.slice(0, separator) : fallbackProvider;
    const model = separator > 0 ? selector.slice(separator + 1) : selector;
    super(`FABRIC_MODEL_NOT_SERVED: Model ${JSON.stringify(model)} is not served by provider ${JSON.stringify(provider)}; nothing launched.${detail ? ` ${detail}` : ""}`);
    this.name = "FabricModelNotServedError";
    this.provider = provider;
    this.model = model;
  }
}
