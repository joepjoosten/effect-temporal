---
"@effect-temporal/client": patch
"@effect-temporal/workflow": patch
---

First-class typed Nexus support in both directions:

- `TemporalNexusOperation.make(service, operation, { payload, success, error })`
  defines a schema-typed Nexus operation shared by handler and callers;
  workflow-side `call` encodes the payload, runs the operation in its own
  cancellation scope, and decodes the backing workflow's typed failure into
  the Effect error channel.
- `TemporalNexusService.workflowRunOperation(operation, workflow)` builds the
  worker `nexusServices` handler backing the operation with an Effect
  workflow — requests start it under its deterministic execution id, with
  `USE_EXISTING` conflict semantics for idempotent duplicate requests.
- The client package's Nexus modules are now typed against the real
  `@temporalio/client` Nexus surface (SDK 1.23) instead of structural
  `unknown` shapes, and construct `NexusClient` directly.
