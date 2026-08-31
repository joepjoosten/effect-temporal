import * as TemporalTesting from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import { activities, runtimeWorkflow } from "./TemporalWorkflowRuntime.e2e-workflows.js"

const workflowsPath = fileURLToPath(new URL("./TemporalWorkflowRuntime.e2e-workflows.ts", import.meta.url))

describe("TemporalWorkflowRuntime e2e", () => {
  it("runs an Effect workflow body through the Temporal sandbox without scheduler timers", async () => {
    const payload = { message: "hello" }

    const runTest = Effect.scoped(
      Effect.gen(function*() {
        const environment = yield* TemporalTesting.makeTimeSkipping()
        const taskQueue = TemporalTesting.makeTaskQueue("effect-runtime-e2e")

        const executeWorkflow = Effect.gen(function*() {
          const result = yield* runtimeWorkflow.execute(payload)
          const executionId = yield* runtimeWorkflow.executionId(payload)
          const client = yield* TemporalTesting.makeClient()
          const events = yield* client.fetchHistoryEvents(`${runtimeWorkflow._tag}/${executionId}`)
          return { events, result }
        }).pipe(
          Effect.provide(TemporalTesting.workflowEngineLayer({ taskQueue })),
          Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment),
          Effect.scoped
        )

        return yield* TemporalTesting.runWorkerUntil(
          {
            activities,
            taskQueue,
            workflowsPath
          },
          () => Effect.runPromise(executeWorkflow)
        ).pipe(
          Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment)
        )
      })
    )

    const { events, result } = await Effect.runPromise(runTest)

    expect(result).toBe("hello:activity-result:child-hello")

    const timerEvents = events.filter((event) => event.timerStartedEventAttributes != null)
    const activityEvents = events.filter((event) => event.activityTaskScheduledEventAttributes != null)
    const childEvents = events.filter(
      (event) => event.startChildWorkflowExecutionInitiatedEventAttributes != null
    )

    // The workflow body forced 25 fiber yields plus activity and child-workflow
    // round trips; fiber scheduling must never surface as durable timers.
    expect(timerEvents).toHaveLength(0)
    expect(activityEvents.length).toBeGreaterThanOrEqual(1)
    expect(childEvents).toHaveLength(1)
  }, 120_000)
})
