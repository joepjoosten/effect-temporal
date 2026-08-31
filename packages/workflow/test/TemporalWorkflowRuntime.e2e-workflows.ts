import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Activity from "effect/unstable/workflow/Activity"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalWorkflowRuntime from "../src/TemporalWorkflowRuntime.js"

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

export const activities = TemporalWorkflowRuntime.makeActivities(runtimeWorkflow, [appendActivity])
