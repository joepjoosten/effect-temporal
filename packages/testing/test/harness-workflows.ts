import * as TemporalWorkflowRuntime from "@effect-temporal/workflow/TemporalWorkflowRuntime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"

export const echoWorkflow = Workflow.make("HarnessEchoWorkflow", {
  payload: {
    message: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ message }) => message
})

export const HarnessEchoWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: echoWorkflow,
  execute: (payload: { readonly message: string }) => Effect.succeed(`echo:${payload.message}`)
})
