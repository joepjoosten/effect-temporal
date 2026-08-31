# @effect-temporal/client

## 0.2.3

### Patch Changes

- [#51](https://github.com/joepjoosten/effect-temporal/pull/51) [`e6b2d8c`](https://github.com/joepjoosten/effect-temporal/commit/e6b2d8c336a5a9902b8a241174045f5fd7c45ccb) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Regenerate the package index so the module documentation headers match the
  retyped Nexus modules.

## 0.2.2

### Patch Changes

- [#49](https://github.com/joepjoosten/effect-temporal/pull/49) [`b7d7ce8`](https://github.com/joepjoosten/effect-temporal/commit/b7d7ce80313e6cbfd79d037f7690bb0032a855ba) Thanks [@joepjoosten](https://github.com/joepjoosten)! - First-class typed Nexus support in both directions:

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

## 0.2.1

### Patch Changes

- [#33](https://github.com/joepjoosten/effect-temporal/pull/33) [`d401a0a`](https://github.com/joepjoosten/effect-temporal/commit/d401a0a1f6acc6e59a066e1d2c2758453e0802c1) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Upgrade effect to 4.0.0-rc.112

- [#33](https://github.com/joepjoosten/effect-temporal/pull/33) [`6fd7326`](https://github.com/joepjoosten/effect-temporal/commit/6fd7326688569f45f4df710aed74d94419c50a8f) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Upgrade Temporal SDK packages to ^1.23.0

## 0.2.0

### Minor Changes

- [#15](https://github.com/joepjoosten/effect-temporal/pull/15) [`0c0b8ec`](https://github.com/joepjoosten/effect-temporal/commit/0c0b8ec1bc7f3f5cd3ce9ceae788b05879288d60) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Add low-level workflow visibility and history APIs to `TemporalWorkflowClient` so consumers no longer need direct `@temporalio/*` imports for these use cases:

  - `listPaged` — page-by-page workflow listing over the raw workflow service with `pageSize`, `limit`, and per-page `onPage` progress reporting, decoded into `WorkflowExecutionInfo`.
  - `fetchHistoryEvents` — fetch a workflow's full raw history, following history pagination.
  - `startInput` — decode the input payloads of the `WorkflowExecutionStarted` event.
  - `decodePayloads` / `decodeFailure` — decode raw history payloads and failures using the client's loaded data converter.
  - Re-export `WorkflowFailedError`, `WorkflowExecutionInfo`, and `ListOptions` from `@temporalio/client`, plus `HistoryEvent`, `RawPayload`, and `RawFailure` proto type aliases.

## 0.1.1

### Patch Changes

- [#13](https://github.com/joepjoosten/effect-temporal/pull/13) [`102b696`](https://github.com/joepjoosten/effect-temporal/commit/102b6965a6126cdbb146c5236aa2408360916dba) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Update the packages to Effect 4.0.0-rc.111 and migrate the test workspace to the Vitest 4 projects configuration.

## 0.1.0

### Minor Changes

- [`6553294`](https://github.com/joepjoosten/effect-temporal/commit/6553294a68b8a8e05321394b785295fd78caac83) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Publish the first public release of the Effect integrations for Temporal.

  - Add Effect services for Temporal connections, clients, workflow handles, and data conversion.
  - Add a Temporal-backed Effect workflow engine, worker lifecycle, workflow runtime, activities, durable deferreds, durable clocks, and child workflows.
  - Add scoped Temporal test environments, workflow engine layers, task queue helpers, and worker helpers.

### Patch Changes

- [#11](https://github.com/joepjoosten/effect-temporal/pull/11) [`e49ad71`](https://github.com/joepjoosten/effect-temporal/commit/e49ad71c6099d5173adc2e4b048d42d14cc8d7bf) Thanks [@joepjoosten](https://github.com/joepjoosten)! - Update all packages to Effect 4 beta.99, adapt workflow definitions to the latest beta API, and keep the testing entry point stable during code generation.
