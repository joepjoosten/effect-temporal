---
"@effect-temporal/workflow": patch
---

Cancellation and compensation semantics for the workflow runtime:

- The workflow body now runs inside a non-cancellable Temporal scope; a
  Temporal cancellation is observed by the Effect runtime — the handler fiber
  is interrupted so finalizers and `withCompensation` handlers run — and the
  run then closes as Cancelled by rethrowing the `CancelledFailure`.
- The reserved interrupt signal now interrupts the live workflow fiber
  (matching Effect's in-memory engine semantics: `instance.interrupted` set,
  suspension cleared), so an interrupt lands as a `Complete` result with an
  interrupted exit after compensation has run, instead of waiting for the
  next suspension point.
- Every activity call gets its own cancellation scope, tracked per run: an
  interrupt or cancellation cancels in-flight scheduled activities, while
  compensation activities scheduled afterwards run in fresh scopes parented
  to the non-cancellable run scope.
- Completed activity results are memoized per attempt in run state, so a
  resumed pass of the workflow body no longer re-schedules (and re-executes)
  activities that already finished.
