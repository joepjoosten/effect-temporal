# `@effect-temporal/workflow`

Run Effect workflows (`effect/unstable/workflow`) on Temporal: a
`WorkflowEngine` implementation, the deterministic sandbox runtime, and the
primitives for entity workflows, typed activities, versioning, and Nexus.

See the [repository README](https://github.com/joepjoosten/effect-temporal)
for the full quick start. [`sample/order-saga`](./sample/order-saga) is a
complete, type-checked sample (definitions / bundle / worker / client)
covering a saga, a long-lived entity, and a Nexus service; the e2e suite in
[`test/`](./test) exercises every feature against a real Temporal test server
and doubles as a second usage reference.

## Modules

| Module                          | Side          | What it does                                                                                                       |
| ------------------------------- | ------------- | ------------------------------------------------------------------------------------------------------------------ |
| `TemporalWorkflowEngine`        | client        | The Effect `WorkflowEngine` over Temporal: start (idempotent, `REJECT_DUPLICATE`), poll, interrupt, resume          |
| `TemporalWorkflowRuntime`       | sandbox       | `makeWorkflow` / `makeActivities`; the run loop, protocol handlers, and the `TemporalSandboxRun` service            |
| `TemporalWorkflowWire`          | shared        | Schema-derived JSON codecs for payloads, results, and deferred exits; the `EffectWorkflowExit` failure format       |
| `TemporalWorkflowProtocol`      | shared        | Reserved `__effect_workflow_*` query/signal names and payload shapes                                                |
| `TemporalSandbox` / `...Polyfills` | sandbox    | Microtask fiber scheduler and sandbox polyfills (SHA-256, `getRandomValues`, `TextEncoder`, pinned `performance`)   |
| `TemporalTypedActivity`         | shared/worker | Activities with explicit payload/success/error schemas and per-activity options; `call` + `handle`/`implement`      |
| `TemporalStateCell`             | shared        | Queryable published workflow state (`set` in the body; read via `TemporalWorkflowInteractions.readStateCell`)       |
| `TemporalDurableMailbox`        | shared        | Typed inbound message channels: `take`/`poll` in the body, workflow-to-workflow `offer`                             |
| `TemporalDurableUpdate`         | shared        | Typed request/response updates with a schema'd failure channel                                                      |
| `TemporalContinueAsNew`         | sandbox       | Continue the workflow as a new run with a schema-encoded payload                                                    |
| `TemporalVersioning`            | sandbox       | `patched`/`deprecatePatch` as Effects and the `match` version-chain combinator                                      |
| `TemporalNexusOperation`        | shared        | Schema-typed Nexus operation definitions and the workflow-side `call`                                               |
| `TemporalNexusService`          | worker        | `workflowRunOperation`: back a Nexus operation with an Effect workflow (`nexusServices` handler)                    |
| `TemporalWorkflowInteractions`  | client        | Client-side entity interactions: `readStateCell`, `offerMailbox`, `executeUpdate`                                   |
| `TemporalWorker`                | worker        | Effect service/Layer for the Temporal worker lifecycle                                                              |
| `TemporalLint`                  | tooling       | ESLint plugin (flat config) enforcing workflow-sandbox safety, with a `recommended` preset                          |
| `TemporalClient` / `TemporalConnection` | client | Re-exports from `@effect-temporal/client` for convenience                                                           |

## Authoring model

Definitions (workflows, typed activities, mailboxes, updates, state cells,
Nexus operations) are plain schema'd values in sandbox-safe modules, shared by
the workflow bundle, the worker, and every client — the sides cannot drift.
The bundle exports Temporal workflow functions via
`TemporalWorkflowRuntime.makeWorkflow`; the worker registers activity
implementations (`TemporalTypedActivity.implement`, or
`TemporalWorkflowRuntime.makeActivities` for `Activity.make` definitions) and
optional `nexusServices`; clients provide `TemporalWorkflowEngine.layer` and
call `workflow.execute` / `poll` / `interrupt` like with any other Effect
engine.

When the client engine uses `workflowIdPrefix`, pass the same prefix to
`TemporalWorkflowRuntime.makeWorkflow` for workflow bodies that start children.
New children use the same `prefix/executionId` (or `workflowTag/executionId`)
address as client interactions. Histories predating the child-ID patch marker
retain their original unprefixed child IDs during replay.

Everything crossing a Temporal boundary is schema-encoded JSON, and typed
failures round-trip: a workflow or activity failing with its schema'd error
lands in the reading side's Effect error channel as a real class instance,
while the run shows as failed in the Temporal UI.

## Determinism

Bodies run on a microtask-based fiber scheduler (fiber yields never create
Temporal timers), sandbox polyfills are installed automatically, and per-run
state (activity/child results, mailbox and update logs) is replayed
deterministically across resumed passes. Application code must still follow
Temporal's determinism rules — apply the `TemporalLint` recommended preset to
workflow bundle files to enforce them, and never reuse the reserved
`__effect_workflow_*` query/signal names.

## Testing

Use `@effect-temporal/testing`: `makeWorkflowTestHarness` boots a test server,
worker, and engine in one scoped call for integration tests, and
`makeStubTemporalClient` records client interactions for serverless unit
tests.
