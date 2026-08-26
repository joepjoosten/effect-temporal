# @effect-temporal/client

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
