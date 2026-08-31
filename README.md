# Effect Temporal

Run [Effect](https://effect.website) workflows (`effect/unstable/workflow`) on
[Temporal](https://temporal.io): author workflows as Effect programs with
schemas and typed errors, and let Temporal own durable execution, replay,
retries, task queues, and the operational tooling around them.

The monorepo publishes three packages:

| Package                    | What it is                                                                                                                              |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `@effect-temporal/workflow` | The `WorkflowEngine` implementation: the client-side engine, the sandbox runtime, entity primitives, typed activities, versioning, Nexus |
| `@effect-temporal/client`   | A general Effect wrapper over `@temporalio/client`: connections, workflow/schedule/activity/Nexus clients as services and layers          |
| `@effect-temporal/testing`  | Test-server environments, a one-call workflow test harness, and a recording stub client for serverless unit tests                        |

## Features

- **A real `WorkflowEngine`** — `TemporalWorkflowEngine.layer` is a drop-in
  replacement for Effect's in-memory or cluster engines. Workflow bodies are
  ordinary Effect programs; Temporal owns durability.
- **Deterministic sandbox runtime** — fiber scheduling runs on the microtask
  queue (never on Temporal timers), and the runtime installs sandbox polyfills
  (`crypto.subtle.digest`, `getRandomValues`, `TextEncoder`, a pinned
  `performance`) byte-pinned against Node, so execution ids hashed in the
  sandbox match ids computed client-side.
- **Typed end to end** — every value crossing a Temporal boundary is
  schema-encoded JSON. A failing workflow fails the Temporal run (red in the
  UI) carrying its encoded exit, and every reading side decodes it back: typed
  errors arrive in the Effect error channel as real schema class instances.
- **Cancellation and compensation** — Temporal cancellations and Effect
  interrupts both interrupt the live fiber, so finalizers and
  `Workflow.withCompensation` run; in-flight activities are cancelled;
  cancelled runs close as Cancelled.
- **Entity workflows** — `TemporalDurableMailbox` (repeated typed inbound
  messages), `TemporalDurableUpdate` (typed request/response with a schema'd
  failure channel), `TemporalStateCell` (queryable published state, readable
  after the run closes), and `TemporalContinueAsNew`.
- **Typed activities** — `TemporalTypedActivity` declares payload, success,
  and failure schemas plus per-activity Temporal options; typed failures skip
  Temporal retries while defects use the retry policy.
- **Versioning** — `TemporalVersioning.match` runs patch-marker version
  chains, letting changed code replay old histories deterministically.
- **Typed Nexus** — schema-typed Nexus operations backed by Effect workflows,
  in both directions (workflow-side caller and worker-side handler).
- **Lint plugin** — `TemporalLint` ships ESLint rules (flat config) that catch
  sandbox-safety mistakes before they become non-deterministic replay
  failures.
- **Idempotency by construction** — a workflow's execution id is a digest of
  its idempotency key; engine starts use `REJECT_DUPLICATE`, so re-executing a
  completed execution id attaches to the recorded result instead of running
  again.

## How it works

Effect workflows are described with `effect/unstable/workflow`: a workflow
name, payload/success/error schemas, an idempotency key, and an Effect program
as the body. The integration maps that surface onto Temporal in layers:

- `TemporalWorkflowEngine` implements the Effect `WorkflowEngine` interface on
  the client side: starting, polling, interrupting, resuming, and completing
  Temporal workflow executions.
- `TemporalWorkflowRuntime.makeWorkflow` turns a workflow definition plus its
  handler into the Temporal workflow function exported from the worker's
  workflow bundle. Inside the isolate it runs the body on a sandbox-safe
  scheduler and serves the reserved protocol queries and signals.
- `TemporalWorkflowWire` defines the wire format: schema-derived JSON codecs
  for payloads, results, and deferred exits, and the `EffectWorkflowExit`
  failure format that carries typed exits through Temporal failures.
- `TemporalWorkflowProtocol` reserves the `__effect_workflow_*` query and
  signal names used by the runtime (state, deferreds, clocks, mailboxes,
  updates, state cells).

Temporal owns durable scheduling and replay; Effect owns the typed workflow
model and dependency graph.

Workflow IDs are derived from the workflow tag and execution id —
`` `${workflowIdPrefix ?? workflowTag}/${executionId}` `` — so a stable Effect
idempotency key becomes a stable Temporal workflow ID.

### Comparison with Effect's own engines

| Engine                         | Backing runtime                                | Best for                                                  |
| ------------------------------ | ---------------------------------------------- | --------------------------------------------------------- |
| `WorkflowEngine.layerMemory`   | Current process memory                         | Tests, local development, single-process workflows        |
| `ClusterWorkflowEngine.layer`  | Effect Cluster, sharding, and `MessageStorage` | Effect-native distributed durable workflows               |
| `TemporalWorkflowEngine.layer` | Temporal server and Temporal workers           | Codebases that already run Temporal                       |

Swapping engines means providing `TemporalWorkflowEngine.layer` on the client
side and running a matching Temporal worker for the same task queue.

## Quick start

### 1. Define the workflow (shared by bundle, worker, and clients)

```ts
// workflows/definitions.ts — keep this file sandbox-safe
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as TemporalTypedActivity from "@effect-temporal/workflow/TemporalTypedActivity"

export class ChargeDeclined extends Schema.TaggedError<ChargeDeclined>()("ChargeDeclined", {
  code: Schema.Number
}) {}

export const orderWorkflow = Workflow.make("OrderWorkflow", {
  payload: { orderId: Schema.String, amountCents: Schema.Number },
  success: Schema.String,
  error: ChargeDeclined,
  idempotencyKey: ({ orderId }) => orderId
})

export const chargeCard = TemporalTypedActivity.make("chargeCard", {
  payload: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Number }),
  success: Schema.Struct({ receiptId: Schema.String }),
  error: ChargeDeclined,
  options: { startToCloseTimeout: "30 seconds", retry: { maximumAttempts: 3 } }
})

export const managerApproval = DurableDeferred.make("manager-approval", {
  success: Schema.Struct({ approverId: Schema.String })
})
```

### 2. Write the workflow bundle (runs inside the Temporal sandbox)

```ts
// workflows/bundle.ts — the file passed to the worker as workflowsPath
import * as Effect from "effect/Effect"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as TemporalTypedActivity from "@effect-temporal/workflow/TemporalTypedActivity"
import * as TemporalWorkflowRuntime from "@effect-temporal/workflow/TemporalWorkflowRuntime"
import { chargeCard, managerApproval, orderWorkflow } from "./definitions.js"

export const OrderWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: orderWorkflow,
  execute: (payload: { readonly orderId: string; readonly amountCents: number }) =>
    Effect.gen(function*() {
      const receipt = yield* TemporalTypedActivity.call(chargeCard, payload)
      const approval = yield* DurableDeferred.await(managerApproval)
      return `${receipt.receiptId}:approved-by:${approval.approverId}`
    })
})
```

### 3. Run the worker

```ts
// worker.ts
import * as TemporalTypedActivity from "@effect-temporal/workflow/TemporalTypedActivity"
import * as TemporalWorker from "@effect-temporal/workflow/TemporalWorker"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { fileURLToPath } from "node:url"
import { chargeCard, ChargeDeclined } from "./workflows/definitions.js"

const activities = TemporalTypedActivity.implement([
  TemporalTypedActivity.handle(chargeCard, (payload) =>
    payload.amountCents <= 0
      ? Effect.fail(new ChargeDeclined({ code: 51 }))
      : Effect.succeed({ receiptId: `receipt-${payload.orderId}` }))
])

const workerLayer = Layer.mergeAll(
  TemporalWorker.connectionLayer({ address: "localhost:7233" }),
  TemporalWorker.layer({
    activities,
    taskQueue: "orders",
    workflowsPath: fileURLToPath(new URL("./workflows/bundle.js", import.meta.url))
  })
)

Effect.runPromise(Effect.provide(
  Effect.gen(function*() {
    const worker = yield* TemporalWorker.TemporalWorker
    yield* worker.run
  }),
  workerLayer
))
```

### 4. Drive it from ordinary Node

```ts
// client.ts
import { TemporalClient, TemporalConnection, TemporalWorkflowEngine } from "@effect-temporal/workflow"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import { orderWorkflow } from "./workflows/definitions.js"

const temporalLayer = Layer.mergeAll(
  TemporalConnection.layer({ address: "localhost:7233" }),
  TemporalClient.layer(),
  TemporalWorkflowEngine.layer({ taskQueue: "orders" })
)

// Typed success and error channels; idempotent by digest id.
const program = orderWorkflow.execute({ orderId: "ord_123", amountCents: 4200 })

Effect.runPromise(Effect.provide(program, temporalLayer))
```

A `ChargeDeclined` raised by the activity or the workflow arrives in the
client's Effect error channel as a real `ChargeDeclined` instance.

For local development: `temporal server start-dev`, then run the worker and
client with `pnpm tsx`.

## Samples

[`packages/workflow/sample/order-saga`](./packages/workflow/sample/order-saga)
is a complete, type-checked sample laid out like a production project —
shared [`definitions.ts`](./packages/workflow/sample/order-saga/definitions.ts),
the sandbox [`workflows.ts`](./packages/workflow/sample/order-saga/workflows.ts)
bundle, a [`worker.ts`](./packages/workflow/sample/order-saga/worker.ts), and a
[`client.ts`](./packages/workflow/sample/order-saga/client.ts) — covering a
one-shot saga (typed activities, compensation, durable timer, approval
deferred, child workflow, versioning) and a long-lived subscription entity
(mailbox, typed updates, state cell, continue-as-new), plus a Nexus-backed
quote service. Run it against `temporal server start-dev` with `pnpm tsx`.

The e2e suite in [`packages/workflow/test`](./packages/workflow/test)
exercises every feature against a real Temporal test server and doubles as a
second usage reference.

## Entity workflows

Long-lived, observable, mutable workflows compose from four primitives, all
schema-typed end to end:

```ts
import * as TemporalDurableMailbox from "@effect-temporal/workflow/TemporalDurableMailbox"
import * as TemporalDurableUpdate from "@effect-temporal/workflow/TemporalDurableUpdate"
import * as TemporalStateCell from "@effect-temporal/workflow/TemporalStateCell"
import * as TemporalContinueAsNew from "@effect-temporal/workflow/TemporalContinueAsNew"

// Workflow side
const message = yield* TemporalDurableMailbox.take(commands)        // durable inbound messages
const request = yield* TemporalDurableUpdate.take(setPlan)          // typed request/response
yield* request.succeed({ plan: "pro" })                             // ... or request.fail(typedError)
yield* TemporalStateCell.set(status, { phase: "active" })           // queryable published state
yield* TemporalContinueAsNew.continueAsNew(entityWorkflow, next)    // bounded histories

// Client side (TemporalWorkflowInteractions)
yield* TemporalWorkflowInteractions.offerMailbox(commands, target, { command: "pause" })
const plan = yield* TemporalWorkflowInteractions.executeUpdate(setPlan, target, { plan: "pro" })
const snapshot = yield* TemporalWorkflowInteractions.readStateCell(status, target) // works after close too
```

`DurableDeferred`, `DurableClock.sleep`, child workflows
(`childWorkflow.execute` in the body), `Workflow.withCompensation`,
interruption, and suspend/resume all work through the engine as well — see
the order-saga sample and the e2e suite for working code.

## Versioning workflow code

```ts
import * as TemporalVersioning from "@effect-temporal/workflow/TemporalVersioning"

TemporalVersioning.match({
  versions: [
    { id: "v1", run: oldBehavior }, // oldest first
    { id: "v2", run: newBehavior }
  ],
  legacy: preVersioningBehavior // for histories predating every version
})
```

New executions record and take the newest branch; replaying executions follow
the branch their history recorded. The e2e replays a generation-1 history
against generation-2 code with `Worker.runReplayHistory` to prove it.

## Nexus

```ts
// Shared definition (sandbox-safe)
const getQuote = TemporalNexusOperation.make("quote-service", "getQuote", {
  payload: QuoteRequest,
  success: Quote,
  error: QuoteRejected
})

// Worker: back the operation with an Effect workflow
const worker = { nexusServices: [TemporalNexusService.workflowRunOperation(getQuote, quoteWorkflow)] }

// Any workflow (cross-namespace): typed call, typed failure channel
const quote = yield* TemporalNexusOperation.call(getQuote, { endpoint: "quotes" }, request)
```

## Testing

`@effect-temporal/testing` provides two levels:

```ts
// Integration: one scoped call boots the test server, a worker, and an engine
const harness = yield* TemporalTesting.makeWorkflowTestHarness({ workflowsPath, activities })
const result = yield* harness.provide(orderWorkflow.execute(payload))

// Unit: drive client-side code with zero servers; every interaction is recorded
const stub = TemporalTesting.makeStubTemporalClient({ resultFor: () => "done" })
// ... Effect.provide(stub.layer); assert on stub.recorded.starts / signals / queries
```

Lower-level building blocks (`makeTimeSkipping`, `makeLocal`, `makeTaskQueue`,
`workflowEngineLayer`, `runWorkerUntil`, ...) remain available for custom
setups.

## Determinism

Workflow bundles run in Temporal's deterministic sandbox. The runtime handles
scheduling and missing globals automatically, but application code must stay
deterministic too: no direct network, filesystem, randomness, or wall-clock
access in workflow code — put side effects in activities. Two guardrails help:

- The reserved `__effect_workflow_*` query/signal names must not be reused by
  application code.
- `TemporalLint` (exported from `@effect-temporal/workflow/TemporalLint`)
  enforces the rest in CI — apply its `recommended` preset to your workflow
  bundle files:

```ts
// eslint.config.mjs
import { plugin as effectTemporal } from "@effect-temporal/workflow/TemporalLint"

export default [
  {
    files: ["src/workflows/**"],
    plugins: { "effect-temporal": effectTemporal },
    rules: effectTemporal.configs.recommended.rules
  }
]
```

## Deployment considerations

- **Determinism:** workflow code is replayed by Temporal; keep it
  deterministic and move side effects into activities (see above).
- **Worker isolation:** code imported by `workflowsPath` runs in the workflow
  isolate. Keep client/worker-side modules and Node-only APIs out of that
  bundle — the `no-mixed-halves` lint rule checks this.
- **Workflow IDs and idempotency:** the same idempotency key resolves to the
  same Temporal workflow ID, and completed execution ids return their recorded
  result (`REJECT_DUPLICATE`). Choose idempotency keys accordingly.
- **Protocol compatibility:** the reserved query/signal payloads are a
  versioned wire format. Upgrade workers and clients together; in-flight runs
  started on an older protocol version are not guaranteed compatible.
- **Task queues:** clients and workers must agree on the task queue. Use
  separate queues for independently deployable worker groups.
- **Versioning:** use `TemporalVersioning.match` when changing workflow
  behavior so old histories keep replaying against new code.

## Operations

```sh
pnpm build   # build all packages
pnpm check   # typecheck
pnpm lint    # lint
pnpm test    # run all tests (live e2e tests boot a Temporal test server)
```

TypeScript files can be executed directly with [tsx](https://tsx.is):
`pnpm tsx ./path/to/file.ts`.
