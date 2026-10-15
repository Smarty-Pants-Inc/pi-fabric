# Mesh lock on macOS (T10 characterization)

The Windows held-object release work does **not** claim macOS parity. macOS keeps the existing weaker, non-Linux release contract until a separately reviewed POSIX object-bound implementation is available.

The hosted `macos-latest` T10 job records only these positive guarantees:

- a matching live mesh or custody owner is not reclaimed merely because it is older than the grace period;
- ordinary release and the supported repeated-release paths work for mesh, custody, async fence, and sync fence entry points;
- an admissibly dead protocol-1 or protocol-2 child can be recovered for mesh and custody.

`tests/mesh-lock-macos-characterization.test.ts` is explicitly hosted-only (`darwin` plus GitHub Actions). Its off-platform skip is not Mac evidence. The dead-holder checks launch and join a plain Node `.mjs` fixture; they do not use Bun, a TypeScript loader, or an inline worker.

The forced replacement case is an **expected failure**. It places a real successor at the old mesh release check and asserts that B survives. Under the current macOS pathname fallback this assertion fails, so Vitest's `it.fails(..., { retry: 0 })` records the known residual. An unexpected pass fails the suite and requires removal/review of the exception. This case is neither positive evidence nor a parity claim.

No macOS result is asserted by the source tree until the hosted T10 job completes. The follow-up remains the implementation lead's tracked work under refs #9569/T10; this change does not invent a follow-up issue number or link.
