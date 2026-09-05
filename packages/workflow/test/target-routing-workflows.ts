import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as DurableClock from "effect/unstable/workflow/DurableClock"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"

const Mode = Schema.Literals(["complete", "interrupt", "interruptUnsafe", "resume", "clock"])
const payload = { id: Schema.String, mode: Mode }
type Payload = { id: string; mode: typeof Mode.Type }
export const approval = DurableDeferred.make("routing-approval", { success: Schema.String })
export const clock = DurableClock.make({ name: "external-clock", duration: 50 })
export const target = Workflow.make("RoutingTarget", {
  payload,
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
export const sender = Workflow.make("RoutingSender", {
  payload,
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
export const RoutingTarget = Runtime.makeWorkflow({
  workflow: target,
  execute: (payload: Payload) =>
    Effect.gen(function*() {
      if (payload.mode === "resume") {
        const state = yield* Runtime.TemporalSandboxRun
        if (!state.stateCells.has("first-pass")) {
          state.stateCells.set("first-pass", true)
          return yield* Workflow.suspend(yield* WorkflowEngine.WorkflowInstance)
        }
        return "resumed"
      }
      if (payload.mode === "clock") {
        yield* DurableDeferred.await(clock.deferred)
        return "woke"
      }
      return yield* DurableDeferred.await(approval)
    })
})
export const RoutingSender = Runtime.makeWorkflow({
  workflow: sender,
  workflowIdPrefix: "custom",
  execute: (payload: Payload) =>
    Effect.gen(function*() {
      const engine = yield* WorkflowEngine.WorkflowEngine
      const executionId = yield* target.executionId(payload)
      if (payload.mode === "complete") {
        const token = yield* DurableDeferred.tokenFromPayload(approval, { workflow: target, payload })
        yield* DurableDeferred.succeed(approval, { token, value: "approved" })
      } else if (payload.mode === "clock") {
        yield* engine.scheduleClock(target, { executionId, clock })
      } else {
        yield* engine[payload.mode](target, executionId)
      }
      return "sent"
    })
})
