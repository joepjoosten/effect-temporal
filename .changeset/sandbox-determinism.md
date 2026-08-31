---
"@effect-temporal/workflow": patch
---

Make the workflow runtime deterministic inside the Temporal sandbox:

- `TemporalWorkflowRuntime.makeWorkflow` now runs workflow bodies on a
  microtask-based fiber scheduler, so fiber yields never fall back to
  `setTimeout` — which is a durable Temporal timer in the sandbox — and no
  longer append timer events to workflow history.
- New `TemporalSandboxPolyfills` module (installed automatically by the
  runtime) provides `crypto.subtle.digest` (SHA-256), `crypto.getRandomValues`,
  and `TextEncoder` inside the workflow isolate, byte-pinned against Node so
  execution ids hashed in the sandbox match ids computed client-side, and pins
  `performance` to the deterministic sandbox clock.
- Fixed workflow identification: the engine and runtime now key workflows by
  `workflow._tag` instead of `workflow.name`, which always evaluated to the
  string `"Workflow"` and broke workflow-type routing end to end.
- Added the first live end-to-end test covering `TemporalWorkflowEngine` +
  `TemporalWorkflowRuntime`: an Effect workflow body with an activity call and
  a child workflow, asserting no scheduler timers appear in history.
