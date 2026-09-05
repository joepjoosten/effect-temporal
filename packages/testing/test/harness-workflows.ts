// Imported by path rather than package name: the worker's webpack bundles
// this file without the workspace build output that the package-name subpath
// would require.
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as DurableClock from "effect/unstable/workflow/DurableClock"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalWorkflowRuntime from "../../workflow/src/TemporalWorkflowRuntime.js"

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

export const timerWorkflow = Workflow.make("HarnessTimerWorkflow", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
export const HarnessTimerWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: timerWorkflow,
  execute: () =>
    Effect.gen(function*() {
      yield* DurableClock.sleep({ name: "one-day", duration: "1 day", inMemoryThreshold: 0 })
      return "done"
    })
})
