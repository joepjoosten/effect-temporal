import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalDurableUpdate from "../src/TemporalDurableUpdate.js"
import * as TemporalWorkflowRuntime from "../src/TemporalWorkflowRuntime.js"

export const increment = TemporalDurableUpdate.make("increment", {
  payload: Schema.Number,
  success: Schema.Number
})

export const counter = Workflow.make("IdempotentCounter", {
  payload: { id: Schema.String },
  success: Schema.Number,
  idempotencyKey: ({ id }) => id
})

export const IdempotentCounter = TemporalWorkflowRuntime.makeWorkflow({
  workflow: counter,
  execute: () =>
    Effect.gen(function*() {
      let total = 0
      for (let i = 0; i < 3; i++) {
        const request = yield* TemporalDurableUpdate.take(increment)
        if (i === 0) {
          yield* Effect.sleep(200)
        }
        total += request.payload
        yield* request.succeed(total)
        // Response capabilities are one-shot, including during resumed passes.
        yield* request.succeed(999)
      }
      return total
    })
})
