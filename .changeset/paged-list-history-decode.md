---
"@effect-temporal/client": minor
---

Add low-level workflow visibility and history APIs to `TemporalWorkflowClient` so consumers no longer need direct `@temporalio/*` imports for these use cases:

- `listPaged` — page-by-page workflow listing over the raw workflow service with `pageSize`, `limit`, and per-page `onPage` progress reporting, decoded into `WorkflowExecutionInfo`.
- `fetchHistoryEvents` — fetch a workflow's full raw history, following history pagination.
- `startInput` — decode the input payloads of the `WorkflowExecutionStarted` event.
- `decodePayloads` / `decodeFailure` — decode raw history payloads and failures using the client's loaded data converter.
- Re-export `WorkflowFailedError`, `WorkflowExecutionInfo`, and `ListOptions` from `@temporalio/client`, plus `HistoryEvent`, `RawPayload`, and `RawFailure` proto type aliases.
