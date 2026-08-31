import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Activity from "effect/unstable/workflow/Activity"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
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

export const activities = TemporalWorkflowRuntime.makeActivities(runtimeWorkflow, [appendActivity])
