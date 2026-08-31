import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Activity from "effect/unstable/workflow/Activity"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalDurableMailbox from "../src/TemporalDurableMailbox.js"
import * as TemporalStateCell from "../src/TemporalStateCell.js"
import * as TemporalWorkflowRuntime from "../src/TemporalWorkflowRuntime.js"

export class OrderRejected extends Schema.TaggedError<OrderRejected>()("OrderRejected", {
  reason: Schema.String,
  code: Schema.Number
}) {}

export const appendActivity = Activity.make({
  name: "appendActivity",
  success: Schema.String,
  execute: Effect.succeed("activity-result")
})

export const childWorkflow = Workflow.make("RuntimeE2EChildWorkflow", {
  payload: {
    name: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ name }) => name
})

export const runtimeWorkflow = Workflow.make("RuntimeE2EWorkflow", {
  payload: {
    message: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ message }) => message
})

export const RuntimeE2EChildWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: childWorkflow,
  execute: (payload: { readonly name: string }) => Effect.succeed(`child-${payload.name}`)
})

export const RuntimeE2EWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: runtimeWorkflow,
  execute: (payload: { readonly message: string }) =>
    Effect.gen(function*() {
      // Force many fiber scheduler yields; with the sandbox scheduler none of
      // these may create Temporal timers in workflow history.
      for (let i = 0; i < 25; i++) {
        yield* Effect.yieldNow
      }
      const activityResult = yield* appendActivity
      const childResult = yield* childWorkflow.execute({ name: payload.message })
      return `${payload.message}:${activityResult}:${childResult}`
    })
})

export const failingWorkflow = Workflow.make("RuntimeE2EFailingWorkflow", {
  payload: {
    reason: Schema.String
  },
  success: Schema.String,
  error: OrderRejected,
  idempotencyKey: ({ reason }) => reason
})

export const RuntimeE2EFailingWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: failingWorkflow,
  execute: (payload: { readonly reason: string }) =>
    Effect.fail(new OrderRejected({ reason: payload.reason, code: 42 }))
})

export const managerApproval = DurableDeferred.make("manager-approval", {
  success: Schema.Struct({ approverId: Schema.String })
})

export const approvalWorkflow = Workflow.make("RuntimeE2EApprovalWorkflow", {
  payload: {
    orderId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ orderId }) => orderId
})

export const RuntimeE2EApprovalWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: approvalWorkflow,
  execute: (payload: { readonly orderId: string }) =>
    Effect.gen(function*() {
      const approval = yield* DurableDeferred.await(managerApproval)
      return `${payload.orderId}:approved-by:${approval.approverId}`
    })
})

// Observed from the test process: reserve/release run as worker-side
// activities in the same Node process as the test.
export const compensationLog: Array<string> = []

export const reserveActivity = Activity.make({
  name: "reserveActivity",
  success: Schema.String,
  execute: Effect.sync(() => {
    compensationLog.push("reserve")
    return "reservation-1"
  })
})

export const releaseActivity = Activity.make({
  name: "releaseActivity",
  execute: Effect.sync(() => {
    compensationLog.push("release")
  })
})

export const neverApproval = DurableDeferred.make("never-approval", {
  success: Schema.String
})

export const compensationWorkflow = Workflow.make("RuntimeE2ECompensationWorkflow", {
  payload: {
    orderId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ orderId }) => orderId
})

export const RuntimeE2ECompensationWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: compensationWorkflow,
  execute: (payload: { readonly orderId: string }) =>
    Effect.gen(function*() {
      const reservation = yield* compensationWorkflow.withCompensation(
        reserveActivity,
        () => releaseActivity
      )
      const approval = yield* DurableDeferred.await(neverApproval)
      return `${payload.orderId}:${reservation}:${approval}`
    })
})

export const orderStatus = TemporalStateCell.make("order-status", {
  value: Schema.Struct({ phase: Schema.String, step: Schema.Number })
})

export const statusWorkflow = Workflow.make("RuntimeE2EStatusWorkflow", {
  payload: {
    orderId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ orderId }) => orderId
})

export const RuntimeE2EStatusWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: statusWorkflow,
  execute: (payload: { readonly orderId: string }) =>
    Effect.gen(function*() {
      yield* TemporalStateCell.set(orderStatus, { phase: "reserved", step: 1 })
      const approval = yield* DurableDeferred.await(managerApproval)
      yield* TemporalStateCell.set(orderStatus, { phase: "approved", step: 2 })
      return `${payload.orderId}:${approval.approverId}`
    })
})

export const orderCommands = TemporalDurableMailbox.make("order-commands", {
  payload: Schema.Struct({ command: Schema.String, sequence: Schema.Number })
})

export const batcherWorkflow = Workflow.make("RuntimeE2EBatcherWorkflow", {
  payload: {
    batchId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ batchId }) => batchId
})

export const RuntimeE2EBatcherWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: batcherWorkflow,
  execute: (payload: { readonly batchId: string }) =>
    Effect.gen(function*() {
      const commands: Array<string> = []
      while (commands.length < 3) {
        const message = yield* TemporalDurableMailbox.take(orderCommands)
        commands.push(`${message.sequence}:${message.command}`)
      }
      return `${payload.batchId}=${commands.join(",")}`
    })
})

export const activities = {
  ...TemporalWorkflowRuntime.makeActivities(runtimeWorkflow, [appendActivity]),
  ...TemporalWorkflowRuntime.makeActivities(compensationWorkflow, [reserveActivity, releaseActivity])
}
