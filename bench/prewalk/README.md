# bench/prewalk — in-place Prewalk evidence harness

Opt-in canary, SWE, and live-evidence tooling for Prewalk in-place sessions.
It is **not** part of `bun run test:affected`, `test:smoke`, or CI.

This sits next to the DeepSWE loop in `bench/`: real or fake-runner
evidence that a model switch is not proof the task finished. Runtime
behavior is covered by `tests/prewalk-*.test.ts` against `src/`.

## Layout

- `canary.md`, `swe.md` — how to run and verify cells
- `prewalk-canary-run.mjs`, `verify-prewalk-canary.mjs`, `prewalk-swe-run.mjs`, …
- `lib/` — parsers, classifiers, snapshot helpers
- `fixtures/` — fake `pi` and SWE workers
- `tests/` — harness tests (Vitest, not in the default include)

## Run the harness tests

From the repo root, with a config that includes this tree:

```sh
bunx vitest run --config bench/prewalk/vitest.config.ts
```

Live paid cells stay opt-in, same idea as Pier: do not put credentials in CI.
