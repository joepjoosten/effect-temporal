---
"@effect-temporal/workflow": patch
---

`TemporalTypedActivity`: schema-typed activities with explicit payloads and
per-activity Temporal options. Define once with
`TemporalTypedActivity.make(name, { payload, success, error, options })`;
the workflow body calls with `TemporalTypedActivity.call(activity, payload)`
— payloads are encoded across the worker boundary and the schema'd failure
is decoded into the Effect error channel — and the worker binds
implementations with `handle` + `implement` (with a `provide` seam for
Effect services). Typed failures complete the Temporal activity without
retries; defects fail it so the activity's retry policy applies. Completed
exits are memoized per call position, so resumed passes of the body replay
results instead of re-scheduling. Options support timeouts, retry policies,
and local activities.
