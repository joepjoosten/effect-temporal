---
"@effect-temporal/workflow": patch
---

Typed wire protocol across all Temporal boundaries. Every value crossing the
isolate boundary is now schema-encoded JSON derived from the workflow's own
payload, success, and error schemas (`TemporalWorkflowWire`):

- Workflow payloads are encoded before start / child-workflow calls and
  decoded inside the sandbox, so schema types (tagged errors, transformations)
  survive the boundary instead of being mangled by the payload converter.
- A failing workflow now fails the Temporal run with a non-retryable
  `ApplicationFailure` of type `EffectWorkflowExit` carrying the encoded
  `Workflow.Result` in `details[0]`. The client engine and the child-workflow
  path decode it back, so typed errors land in the Effect error channel as
  real schema class instances, and runs show as failed in the Temporal UI.
- Durable-deferred exits ride signals and queries as canonical JSON
  (`encodeDeferredExit` / `decodeDeferredExit`) instead of raw structural
  `Exit` objects.
- `poll` now decodes the suspended workflow state, returning
  `Workflow.Suspended` including its cause instead of always `undefined`.

This changes the reserved query/signal payload formats, so workers and clients
must be upgraded together; in-flight workflow runs started on the previous
protocol are not compatible.
