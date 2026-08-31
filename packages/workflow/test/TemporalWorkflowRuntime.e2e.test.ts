import * as TemporalTesting from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import type * as Workflow from "effect/unstable/workflow/Workflow"
import { fileURLToPath } from "node:url"
import * as TemporalClient from "../src/TemporalClient.js"
import * as TemporalWorkflowInteractions from "../src/TemporalWorkflowInteractions.js"
import {
  activities,
  addToCounter,
  approvalWorkflow,
  batcherWorkflow,
  chargeLog,
  compensationLog,
  compensationWorkflow,
  countdownWorkflow,
  counterWorkflow,
  failingWorkflow,
  managerApproval,
  NegativeAmount,
  orderCommands,
  OrderRejected,
  orderStatus,
  paymentWorkflow,
  runtimeWorkflow,
  statusWorkflow
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

  it("runs compensation when a suspended workflow is interrupted", async () => {
    const payload = { orderId: "ord-interrupt" }
    compensationLog.length = 0

    const result = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const executionId = yield* compensationWorkflow.executionId(payload)
        yield* compensationWorkflow.execute(payload, { discard: true })

        yield* pollUntil(
          compensationWorkflow.poll(executionId),
          (status) => Option.isSome(status) && status.value._tag === "Suspended"
        )
        yield* compensationWorkflow.interrupt(executionId)

        const completed = yield* pollUntil(
          compensationWorkflow.poll(executionId),
          (status) => Option.isSome(status) && status.value._tag === "Complete"
        )
        return Option.getOrThrow(completed) as Workflow.Result<string, never>
      }).pipe(Effect.orDie)
    )

    expect(result._tag).toBe("Complete")
    const exit = (result as Workflow.Result<string, never> & { _tag: "Complete" }).exit
    expect(Exit.isFailure(exit)).toBe(true)
    expect(Cause.hasInterruptsOnly((exit as Exit.Failure<string, never>).cause)).toBe(true)

    expect(compensationLog).toContain("reserve")
    expect(compensationLog).toContain("release")
    // Activity memoization: the resumed pass must not re-run the reserve
    // activity before compensation.
    expect(compensationLog.filter((entry) => entry === "reserve")).toHaveLength(1)
  }, 120_000)

  it("publishes typed state cells readable while running and after completion", async () => {
    const payload = { orderId: "ord-status" }

    const { finalStatus, result, suspendedStatus } = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const executionId = yield* statusWorkflow.executionId(payload)
        const target = { executionId, workflow: statusWorkflow }
        const client = yield* TemporalTesting.makeClient()
        const readStatus = TemporalWorkflowInteractions.readStateCell(orderStatus, target).pipe(
          Effect.provideService(TemporalClient.TemporalWorkflowClient, client)
        )

        yield* statusWorkflow.execute(payload, { discard: true })
        yield* pollUntil(
          statusWorkflow.poll(executionId),
          (status) => Option.isSome(status) && status.value._tag === "Suspended"
        )
        const suspendedStatus = yield* readStatus

        const token = yield* DurableDeferred.tokenFromPayload(managerApproval, {
          workflow: statusWorkflow,
          payload
        })
        yield* DurableDeferred.succeed(managerApproval, {
          token,
          value: { approverId: "boss" }
        })
        const completed = yield* pollUntil(
          statusWorkflow.poll(executionId),
          (status) => Option.isSome(status) && status.value._tag === "Complete"
        )
        const finalStatus = yield* readStatus
        return { finalStatus, result: Option.getOrThrow(completed) as Workflow.Result<string, never>, suspendedStatus }
      }).pipe(Effect.orDie)
    )

    expect(Option.getOrThrow(suspendedStatus)).toEqual({ phase: "reserved", step: 1 })
    expect(Option.getOrThrow(finalStatus)).toEqual({ phase: "approved", step: 2 })
    expect(result._tag).toBe("Complete")
    expect((result as Workflow.Result<string, never> & { _tag: "Complete" }).exit).toEqual(
      Exit.succeed("ord-status:boss")
    )
  }, 120_000)

  it("delivers typed mailbox messages to a durably waiting workflow", async () => {
    const payload = { batchId: "batch-1" }

    const result = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const executionId = yield* batcherWorkflow.executionId(payload)
        const target = { executionId, workflow: batcherWorkflow }
        const client = yield* TemporalTesting.makeClient()

        yield* batcherWorkflow.execute(payload, { discard: true })
        for (const [sequence, command] of [[1, "reserve"], [2, "charge"], [3, "ship"]] as const) {
          yield* TemporalWorkflowInteractions.offerMailbox(orderCommands, target, { command, sequence }).pipe(
            Effect.provideService(TemporalClient.TemporalWorkflowClient, client)
          )
        }

        return yield* batcherWorkflow.execute(payload)
      }).pipe(Effect.orDie)
    )

    expect(result).toBe("batch-1=1:reserve,2:charge,3:ship")
  }, 120_000)

  it("serves durable updates with typed success and typed failure responses", async () => {
    const payload = { counterId: "counter-1" }

    const { first, rejected, result, second } = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const executionId = yield* counterWorkflow.executionId(payload)
        const target = { executionId, workflow: counterWorkflow }
        const client = yield* TemporalTesting.makeClient()
        const sendUpdate = (amount: number) =>
          TemporalWorkflowInteractions.executeUpdate(addToCounter, target, { amount }).pipe(
            Effect.provideService(TemporalClient.TemporalWorkflowClient, client)
          )

        yield* counterWorkflow.execute(payload, { discard: true })
        const first = yield* sendUpdate(5)
        const rejected = yield* sendUpdate(-2).pipe(Effect.flip)
        const second = yield* sendUpdate(3)
        // Re-executing a completed execution id must attach to the recorded
        // result, not start a fresh run (REJECT_DUPLICATE reuse policy).
        const result = yield* counterWorkflow.execute(payload)
        return { first, rejected, result, second }
      }).pipe(Effect.orDie)
    )

    expect(first).toEqual({ total: 5 })
    expect(rejected).toBeInstanceOf(NegativeAmount)
    expect((rejected as NegativeAmount).amount).toBe(-2)
    expect(second).toEqual({ total: 8 })
    expect(result).toBe("counter-1=8")
  }, 120_000)

  it("continues as new across runs and follows the chain to the final result", async () => {
    const payload = { chainId: "chain-1", remaining: 3, total: 0 }

    const { events, result } = await runWithWorker((_taskQueue) =>
      Effect.gen(function*() {
        const result = yield* countdownWorkflow.execute(payload)
        const executionId = yield* countdownWorkflow.executionId(payload)
        const client = yield* TemporalTesting.makeClient()
        const events = yield* client.fetchHistoryEvents(`${countdownWorkflow._tag}/${executionId}`)
        return { events, result }
      }).pipe(Effect.orDie)
    )

    // 3 + 2 + 1 accumulated across three continue-as-new hops.
    expect(result).toBe("chain-1=6")
    // The latest run's history starts from a continued-as-new execution.
    const started = events.find((event) => event.workflowExecutionStartedEventAttributes != null)
    expect(started?.workflowExecutionStartedEventAttributes?.continuedExecutionRunId).toBeTruthy()
  }, 120_000)

  it("calls typed activities with explicit payloads and typed failure channels", async () => {
    const payload = { orderId: "ord-pay" }
    chargeLog.length = 0

    const result = await runWithWorker((_taskQueue) => paymentWorkflow.execute(payload).pipe(Effect.orDie))

    expect(result).toBe("receipt-ord-pay:declined-51")
    // Both calls reached the worker with decoded payloads.
    expect(chargeLog).toEqual(["ord-pay:500", "ord-pay:-1"])
  }, 120_000)
})
