---
"@effect-temporal/workflow": patch
---

`TemporalLint`: an ESLint plugin (flat-config compatible) enforcing
workflow-sandbox safety on workflow bundle files, with a `recommended`
preset. Rules: `no-module-level-mutable` (mutable module-scope state breaks
V8 context reuse and replay), `no-mixed-halves` (no client/worker-side or
Node imports in bundle files; forbidden patterns configurable),
`prefer-typed-activity` (flag direct `proxyActivities` usage), and
`zero-arity-effect-promise` (thunks passed to `Effect.promise` /
`Effect.tryPromise` must take no parameters).
