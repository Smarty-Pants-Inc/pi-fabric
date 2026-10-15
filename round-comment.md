Scope decision: the concurrent diagnostics append/rotate size-check race is exactly Refs Smarty-Pants-Inc/smarty-dev#13199's tracked issue and remains out of scope for this round; `src/core/diagnostics-log.ts` is unchanged.

Principal console sweep: the only remaining `console.warn`/`console.error` call sites are `src/core/diagnostics.ts:96-97`, which are the intentional non-principal fallback inside `!isPrincipalDiagnostics()`; the `src/topology/control-plane.ts:374` and `src/topology/participant-directory.ts:682` matches are documentation comments, not calls. Principal-reachable Fabric diagnostics now use the private diagnostics helper, including the parked actor recovery path. The fixture compatibility refusal uses `process.stderr.write` because it is an explicit safety refusal that must remain visible to the fixture user, not an internal diagnostic.

Test model: no task agents were used before the final audit; final audit model will be recorded here when its verdict arrives.
