import { describe, expect, it } from "@effect/vitest"
import { type WorkflowClient, WorkflowFailedError, WorkflowNotFoundError } from "@temporalio/client"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as DurableClock from "effect/unstable/workflow/DurableClock"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as Client from "../src/TemporalClient.js"
import * as Engine from "../src/TemporalWorkflowEngine.js"

const workflow = Workflow.make("EngineErrors", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
const deferred = DurableDeferred.make("approval", { success: Schema.String })
const instance = WorkflowEngine.WorkflowInstance.initial(workflow, "id")
const make = (cause: Error, status?: string) => {
  const reject = async () => {
    throw cause
  }
  const handle = {
    workflowId: "id",
    signal: reject,
    query: reject,
    result: reject,
    describe: status === undefined ? reject : async () => ({ status: { name: status } })
  }
  const raw = { getHandle: () => handle, start: async () => handle, result: reject } as unknown as WorkflowClient
  return Effect.runPromise(
    Engine.make({ taskQueue: "q" }).pipe(
      Effect.provideService(Client.TemporalWorkflowClient, Client.fromUnsafe(raw))
    )
  )
}
const operations = (engine: WorkflowEngine.WorkflowEngine["Service"]): Record<
  string,
  Effect.Effect<unknown, never, WorkflowEngine.WorkflowInstance>
> => ({
  poll: engine.poll(workflow, "id"),
  interrupt: engine.interrupt(workflow, "id"),
  interruptUnsafe: engine.interruptUnsafe(workflow, "id"),
  resume: engine.resume(workflow, "id"),
  deferredResult: engine.deferredResult(deferred),
  deferredDone: engine.deferredDone(deferred, {
    workflowName: workflow._tag,
    executionId: "id",
    deferredName: deferred.name,
    exit: Exit.succeed("approved")
  }),
  scheduleClock: engine.scheduleClock(workflow, {
    executionId: "id",
    clock: DurableClock.make({ name: "timer", duration: 100 })
  })
})
const runExit = (effect: Effect.Effect<unknown, never, WorkflowEngine.WorkflowInstance>) =>
  Effect.runPromiseExit(effect.pipe(Effect.provideService(WorkflowEngine.WorkflowInstance, instance)))

describe("workflow engine RPC errors", () => {
  it.each(["UNAVAILABLE", "PERMISSION_DENIED", "query handler missing"])("preserves %s failures", async (message) => {
    const cause = new Error(message)
    for (const status of [undefined, "RUNNING", "FAILED"]) {
      const engine = await make(cause, status)
      for (const [name, operation] of Object.entries(operations(engine))) {
        const exit = await runExit(operation)
        expect(Exit.isFailure(exit), `${status}/${name}`).toBe(true)
        if (Exit.isFailure(exit)) {
          const defect = exit.cause.reasons.find(Cause.isDieReason)?.defect
          expect(defect).toHaveProperty("cause", cause)
        }
      }
    }
  })

  it("treats missing reads/controls as idempotent but does not lose completions or clocks", async () => {
    const engine = await make(new WorkflowNotFoundError("missing or closed", "id", undefined))
    for (const [name, operation] of Object.entries(operations(engine))) {
      const exit = await runExit(operation)
      expect(exit._tag, name).toBe(name === "deferredDone" || name === "scheduleClock" ? "Failure" : "Success")
      if (Exit.isSuccess(exit) && (name === "poll" || name === "deferredResult")) {
        expect(exit.value).toEqual(Option.none())
      }
    }
  })

  it("returns recorded workflow failures as outcomes while RPC failures remain defects", async () => {
    const failure = new WorkflowFailedError("failed", new Error("workflow defect"), "NON_RETRYABLE_FAILURE")
    const engine = await make(failure, "FAILED")
    const result = await Effect.runPromise(engine.poll(workflow, "id"))
    expect(Option.isSome(result)).toBe(true)
    if (Option.isSome(result)) {
      expect(result.value._tag).toBe("Complete")
      if (result.value._tag === "Complete") expect(Exit.isFailure(result.value.exit)).toBe(true)
    }
  })
})
