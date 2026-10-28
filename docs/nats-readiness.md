# NATS startup and readiness

Fabric's long-lived runtime transport retries a failed **first** NATS connection without restarting Pi. That caller explicitly opts in with `retryFirstConnect: true`, selecting the client's native `waitOnFirstConnect: true` while preserving `reconnect: true`, `maxReconnectAttempts: -1`, and Fabric's existing exponential reconnect delay. The connection helper defaults to `retryFirstConnect: false`: admin commands, auxiliary connections, and one-shot opens still fail fast rather than waiting indefinitely. This does not change TLS policy.

For an opted-in connection, `timeoutMs` bounds each address's connection/handshake attempt, not the total startup wait; DNS resolution precedes that timer.

An ordinary NATS-backed session starts with `NATS: NOT CONNECTED` while Fabric activates in the background. Other Pi extensions and the session can continue starting. Operations that need Fabric still await its activation. Native resident-ready acknowledgements, stopped-task result replay, and successful self-reload reporting continue to wait for completed activation.

## Readiness probe

The runtime's verified connection owns this local marker:

```text
<agentDir>/fabric/nats-ready-<Pi process PID>
```

`agentDir` is the profile directory selected by Pi (for example, `PI_CODING_AGENT_DIR`). Use the PID of the Pi process being probed, **not** the probe process's PID. Each process has a different marker, so a child agent cannot make its parent appear ready.

For an entrypoint that records the Pi PID as `PI_PID` and sets `PI_CODING_AGENT_DIR`, an exec readiness probe can use:

```sh
kill -0 "$PI_PID" && test -f "$PI_CODING_AGENT_DIR/fabric/nats-ready-$PI_PID"
```

Both variables must identify the target process and its actual profile directory. Checking process existence also rejects a marker left behind by an ungraceful process exit. Do not probe for any matching `nats-ready-*` file: that could select another process or an old marker.

The marker is absent while the initial connection is waiting and while `USER.INFO` identity/permission verification is pending. It appears only after connection verification and JetStream manager setup succeed. It is removed when the connection is aborted, disconnected, unverified during reconnect, explicitly closed, or terminally lost; a reconnect restores it only after verification succeeds again. This is a **verified NATS connection** signal, not a guarantee that presence publication or every Fabric component has finished activating.

Configure readiness separately from liveness. An unavailable broker should keep the pod **NotReady** while the native client retries; restarting a live Pi process is not the retry mechanism. This repository exposes the signal; deployment manifests must wire the probe to the owning process.

## Retry diagnostics and cancellation

Failed server attempts produce a rate-limited line containing an error code (and errno when present) plus the phase when known: DNS, TCP, TLS, INFO, authentication, or connection/handshake. The diagnostics do not print credentials, protocol frames, server URLs, or raw error messages.

The first-connect adapter is isolated to the pinned NATS client version. It retains nats.js's native dial loop and reconnect policy; it does not add another retry loop. Its additional ownership hooks make an aborted startup stop pending sockets and retries, including continuations that finish after cancellation. Revalidate the adapter seams and real-socket tests before upgrading the NATS packages.

For opted-in runtime connections, TLS transport failures remain retryable, including certificate failures. They do not turn into a total startup deadline merely because `timeoutMs` is set. Cancel the connection owner to stop waiting; correct trust and hostname configuration before expecting readiness. One-shot opens retain their default fail-fast behavior. Server authentication and `USER.INFO` failures retain their existing terminal verification behavior.
