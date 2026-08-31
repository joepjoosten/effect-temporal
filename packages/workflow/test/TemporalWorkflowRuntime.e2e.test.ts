import * as TemporalTesting from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import type * as Workflow from "effect/unstable/workflow/Workflow"
import { fileURLToPath } from "node:url"
import {
  activities,
  approvalWorkflow,
  failingWorkflow,
  managerApproval,
  OrderRejected,
  runtimeWorkflow
} from "./TemporalWorkflowRuntime.e2e-workflows.js"

const workflowsPath = fileURLToPath(new URL("./TemporalWorkflowRuntime.e2e-workflows.ts", import.meta.url))

const runWithWorker = <A>(
  build: (taskQueue: string) => Effect.Effect<A, never, any>
): Promise<A> =>
  Effect.runPromise(Effect.scoped(
    Effect.gen(function*() {
      const environment = yield* TemporalTesting.makeTimeSkipping()
      const taskQueue = TemporalTesting.makeTaskQueue("effect-runtime-e2e")
      const executeProgram = build(taskQueue).pipe(
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
        () => Effect.runPromise(executeProgram as Effect.Effect<A>)
      ).pipe(
        Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment)
      )
    })
  ))

const pollUntil = <A>(
  poll: Effect.Effect<A, unknown, any>,
  predicate: (value: A) => boolean
): Effect.Effect<A, unknown, any> =>
  Effect.gen(function*() {
    for (let attempt = 0; attempt < 100; attempt++) {
      const value = yield* poll
      if (predicate(value)) {
        return value
      }
      yield* Effect.sleep(100)
    }
    return yield* Effect.die(new Error("pollUntil: condition not reached"))
  })

describe("TemporalWorkflowRuntime e2e", () => {
  it("runs an Effect workflow body through the Temporal sandbox without scheduler timers", async () => {
    const payload = { message: "hello" }

    const { events, result } = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const result = yield* runtimeWorkflow.execute(payload)
        const executionId = yield* runtimeWorkflow.executionId(payload)
        const client = yield* TemporalTesting.makeClient()
        const events = yield* client.fetchHistoryEvents(`${runtimeWorkflow._tag}/${executionId}`)
        return { events, result }
      }).pipe(Effect.orDie)
    )

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

  it("lands typed workflow failures in the client error channel", async () => {
    const error = await runWithWorker((_taskQueue) =>
      failingWorkflow.execute({ reason: "card-declined" }).pipe(
        Effect.flip,
        Effect.orDie
      )
    )

    expect(error).toBeInstanceOf(OrderRejected)
    expect(error.reason).toBe("card-declined")
    expect(error.code).toBe(42)
  }, 120_000)

  it("suspends on a durable deferred and resumes with the typed completion", async () => {
    const payload = { orderId: "ord-1" }

    const result = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const executionId = yield* approvalWorkflow.executionId(payload)
        yield* approvalWorkflow.execute(payload, { discard: true })

        yield* pollUntil(
          approvalWorkflow.poll(executionId),
          (status) => Option.isSome(status) && status.value._tag === "Suspended"
        )

        const token = yield* DurableDeferred.tokenFromPayload(managerApproval, {
          workflow: approvalWorkflow,
          payload
        })
        yield* DurableDeferred.succeed(managerApproval, {
          token,
          value: { approverId: "boss" }
        })

        const completed = yield* pollUntil(
          approvalWorkflow.poll(executionId),
          (status) => Option.isSome(status) && status.value._tag === "Complete"
        )
        return Option.getOrThrow(completed) as Workflow.Result<string, never>
      }).pipe(Effect.orDie)
    )

    expect(result._tag).toBe("Complete")
    expect((result as Workflow.Result<string, never> & { _tag: "Complete" }).exit).toEqual(
      Exit.succeed("ord-1:approved-by:boss")
    )
  }, 120_000)
})
