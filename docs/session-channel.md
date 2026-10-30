# NATS session channel

The session channel lets an in-process Pi extension expose request/reply operations and JSON events for one live Fabric session. It is available only with the NATS mesh transport and a configured `mesh.nats.org`.

## Obtain the API from another extension

Pi gives every extension factory its own `ExtensionAPI` object. The `pi.events` bus is shared across those objects, so consumers use the order-independent handoff; `pi.fabric` on their own object will not expose Fabric's API. `pi.fabric.sessionChannel` is also exposed on Fabric's own `pi` object.

```ts host
import { requestSessionChannelApi } from "pi-fabric";

export default (pi) => {
  const ready = requestSessionChannelApi(pi);
  let channel;

  pi.on("session_start", async () => {
    const fabric = await ready;
    channel = await fabric.open({ service: "smarty-code-ordinary", version: 1 });
    channel.handle("snapshot", async ({ body, caller, requestId }) => ({
      status: 200,
      body: { requestId, caller: caller.id, snapshot: body },
    }));
  });

  pi.on("session_shutdown", () => channel?.close());
};
```

The helper listens for `fabric:session-channel:ready` and emits `fabric:session-channel:get`; Fabric registers the responder and emits readiness. Either extension can load first. The API object is retrieved during factory load; open the session channel after `session_start` when Fabric has initialized that session.

A service is one lowercase NATS token matching `^[a-z0-9][a-z0-9-]{0,47}$`. `events` is reserved. The supported request operations are `snapshot`, `history`, `input`, `model`, and `abort`.

## Subjects and wire format

Requests use:

```
fabric.svc.<org>.<host-token>.<session-token>.<service>.<operation>
```

Events use the same prefix ending in `.events`. Host and session tokens are the first 32 lowercase hexadecimal characters of SHA-256 over the UTF-8 host ID and session ID. For example, an example vector for org `example-org`, host ID `session-channel-host`, session ID `11111111-2222-4333-8444-555555555555`, and service `smarty-code-ordinary` is:

```
fabric.svc.example-org.e8f6226427beb35d340fd1f431ce5441.cf4c4732fd3b8f8a55b60871950a2f22.smarty-code-ordinary.snapshot
```

A request carries JSON in its body, a UUID in `Fabric-Request-Id`, and `Fabric-Caller` as the exact JSON object `{"id":"<caller>"}`. The reply is JSON `{ "status": number, "body": unknown }`. The request budget is 256 KiB. The 1 MiB reply budget applies to the complete encoded `{status, body}` object.

Invalid caller or request ID returns 400, an unhandled operation returns 404, an oversized request returns 413, and a handler failure or oversized reply returns 500. If a gateway maps its NATS request timeout to HTTP 504, that means the outcome is unknown: the handler may have completed even if its reply did not arrive. The channel itself does not synthesize an HTTP response.

The in-memory replay cache is keyed by `(caller id, operation, request id)`. Concurrent duplicates share one pending handler; completed responses are retained for 10 minutes in a 1,000-entry LRU. In-flight entries are not evicted. If the cache is full of in-flight entries, a new request receives no reply. The cache is process-local and is cleared by restart; it is not stored in NATS KV and does not promise dedupe across restarts.

## Alpha trust boundary

Alpha uses a shared org user for the session channel (Paul, 10-20). The runtime role publishes and subscribes on `fabric.svc.<org>.>` and has response permission for up to 16 concurrent in-flight replies per connection. NATS 2.14.7 reports this as `responses: { max: 16, ttl: 120000000000 }` (the server's default 120-second TTL). This is org-scoped, not session-isolated: same-org runtime callers with those grants can read service requests and race to reply. That is an accepted alpha trust assumption; do not treat the channel as authorization between agents in one org.

## Discovering advertised services

A non-Fabric client discovers a root session through the NATS KV bucket `fabric_presence`. For a machine the client is authorized to read, enumerate keys matching:

```
<machine>.p.>
```

The exact participant key is `<machine>.p.<token>`, where `<token>` is the first 32 lowercase hexadecimal characters of SHA-256 over the UTF-8 participant `id`. Its KV subject is `$KV.fabric_presence.<machine>.p.<token>`. The machine token is the NATS machine identity; it is not inferred from the JSON record. Current runtime permissions scope presence reads to their machine prefix.

A KV entry is a JSON envelope `{ "owner": "<ownerHostId>#<ownerIncarnation>", "value": { ... } }`. For a root, use these `value` fields:

- `id`: participant ID;
- `ownerHostId`: the owning Fabric host ID (`hostId`);
- `sessionId`: the Pi session ID;
- `services`: an optional array of `{ "name": string, "version": number }` advertisements.

Treat a missing `services` field as no advertised services. Match the service name and version, then form its request/event subject from the published org, host ID, session ID, and service.
