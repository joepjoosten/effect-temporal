# `@effect-temporal/testing`

Testing helpers for `@effect-temporal/workflow`, at three levels: a one-call
integration harness, a recording stub client for serverless unit tests, and
lower-level building blocks over `@temporalio/testing`.

## Integration tests: the workflow test harness

`makeWorkflowTestHarness` boots a Temporal test environment (time-skipping by
default, `local` for features like Nexus), runs a worker for your workflow
bundle and activities for the lifetime of the scope, and hands back a ready
client, engine, and a `provide` helper:

```ts
import * as TemporalTesting from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import { activities } from "./activities.js"
import { orderWorkflow } from "./definitions.js"

const workflowsPath = fileURLToPath(new URL("./workflows.ts", import.meta.url))

describe("order workflow", () => {
  it("completes", async () => {
    const result = await Effect.runPromise(Effect.scoped(
      Effect.gen(function*() {
        const harness = yield* TemporalTesting.makeWorkflowTestHarness({ activities, workflowsPath })
        return yield* harness.provide(orderWorkflow.execute({ orderId: "ord_1", amountCents: 4200 }))
      })
    ))
    expect(result).toContain("receipt-ord_1")
  })
})
```

Everything — server, worker, connections — is torn down with the scope, in
the right order.

## Unit tests: the recording stub client

`makeStubTemporalClient` implements the workflow-client surface as a test
double with zero servers. Every start, signal, query, termination, and
cancellation is recorded for assertions; any member you did not configure
throws a descriptive error naming it instead of returning `undefined`:

```ts
const stub = TemporalTesting.makeStubTemporalClient({
  resultFor: () => "recorded-result",
  alreadyStartedWorkflowIds: ["OrderWorkflow/abc123"] // simulate attach paths
})

const result = await Effect.runPromise(
  program.pipe(Effect.provide(stub.layer)) // program uses TemporalWorkflowClient
)

expect(stub.recorded.starts[0].workflow).toBe("OrderWorkflow")
```

This is enough to drive the real `TemporalWorkflowEngine` in unit tests,
including its idempotent attach behavior.

## Building blocks

For custom setups, the lower-level constructors and layers remain available
and automatically wire the test environment address and namespace into the
package services:

- `makeTimeSkipping` / `makeLocal` — scoped `TestWorkflowEnvironment`s
- `makeTaskQueue(prefix)` — unique task-queue names per test
- `makeConnection` / `makeClient` / `makeClientWithDataConverter`
- `makeWorkerConnection` / `makeWorker` / `runWorkerUntil`
- `connectionLayer` / `clientLayer` / `workflowEngineLayer` / `workerLayer`

```ts
const program = Effect.scoped(Effect.gen(function*() {
  const environment = yield* TemporalTesting.makeTimeSkipping()
  const taskQueue = TemporalTesting.makeTaskQueue("checkout")

  const executeWorkflow = checkoutWorkflow.execute(payload).pipe(
    Effect.provide(TemporalTesting.workflowEngineLayer({ taskQueue })),
    Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment),
    Effect.scoped
  )

  return yield* TemporalTesting.runWorkerUntil(
    { taskQueue, workflowsPath },
    () => Effect.runPromise(executeWorkflow)
  ).pipe(
    Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment)
  )
}))
```
