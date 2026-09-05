import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Schema from "effect/Schema"
import * as Activity from "effect/unstable/workflow/Activity"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as Typed from "../src/TemporalTypedActivity.js"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"

const Mode = Schema.Literals(["race", "timeout", "interrupt", "success"])
export const workflow = Workflow.make("FiberCancellation", {
  payload: { mode: Mode, legacy: Schema.Boolean },
  success: Schema.String,
  idempotencyKey: ({ legacy, mode }) => `${legacy}/${mode}`
})
export const slow = Typed.make("cancellableActivity", { payload: Schema.String, success: Schema.String })
export const legacy = Activity.make({
  name: "legacyCancellableActivity",
  success: Schema.String,
  execute: Effect.sleep(200).pipe(Effect.as("done"))
})
export const FiberCancellation = Runtime.makeWorkflow({
  workflow,
  execute: (payload: { mode: typeof Mode.Type; legacy: boolean }) =>
    Effect.gen(function*() {
      const call = Effect.gen(function*() {
        if (payload.legacy) return yield* legacy
        return yield* Typed.call(slow, "input")
      })
      if (payload.mode === "race") {
        yield* Effect.raceFirst(call, Effect.sleep(30))
      } else if (payload.mode === "timeout") {
        yield* call.pipe(Effect.timeoutOption(30))
      } else if (payload.mode === "interrupt") {
        const fiber = yield* Effect.forkChild(call)
        yield* Effect.sleep(30)
        yield* Fiber.interrupt(fiber)
      } else {
        yield* call
      }
      // Keep the run open so cancellation is observed independently of parent closure.
      yield* Effect.sleep(250)
      return "done"
    })
})
