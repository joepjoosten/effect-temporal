---
"@effect-temporal/testing": patch
---

Two new testing utilities:

- `makeStubTemporalClient` — a recording stub (test double) for the Temporal
  workflow client, for unit tests with zero servers. Every start, signal,
  query, termination, and cancellation is recorded for assertions;
  unconfigured surface members throw a descriptive error naming the member;
  `alreadyStartedWorkflowIds` simulates already-started executions so the
  engine's attach path can be exercised. Ships a ready `layer`.
- `makeWorkflowTestHarness` — a one-call, scope-bound integration harness:
  boots the test environment (time-skipping by default, `local` selectable),
  runs a worker for the given workflow bundle and activities for the
  lifetime of the scope with ordered teardown, and returns a ready client,
  engine, and a `provide` helper wiring both into any Effect.
