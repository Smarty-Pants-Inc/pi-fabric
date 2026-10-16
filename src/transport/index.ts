import { openFileTransport } from "./file.js";
import {
  TransportInvalidError,
  type FabricTransport,
  type FabricTransportFactory,
  type FabricTransportOptions,
} from "./types.js";

/** Open the selected carrier once; failures never fall back to another carrier. */
export async function openFabricTransport(
  options: FabricTransportOptions,
  factory?: FabricTransportFactory,
): Promise<FabricTransport> {
  options.signal?.throwIfAborted();
  // A2 owns the config field. Keep runtime validation until that type lands, too.
  const config = options.config as FabricTransportOptions["config"] & { readonly transport?: unknown };
  const kind = config.transport === undefined ? "file" : config.transport;
  if (kind !== "file" && kind !== "nats") {
    throw new TransportInvalidError("mesh.transport must be file or nats");
  }
  if (factory) return factory(options);
  if (kind === "file") return openFileTransport(options);

  const { openNatsTransport } = await import("./nats/index.js");
  options.signal?.throwIfAborted();
  return openNatsTransport(options);
}
