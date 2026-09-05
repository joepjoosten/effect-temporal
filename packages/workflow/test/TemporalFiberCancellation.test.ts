import { makeWorkflowTestHarness } from "@effect-temporal/testing/TemporalWorkflowHarness"
import { describe, expect, it } from "@effect/vitest"
import { Worker } from "@temporalio/worker"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import * as Typed from "../src/TemporalTypedActivity.js"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"
import { legacy, slow, workflow } from "./fiber-cancellation-workflows.js"

const workflowsPath = fileURLToPath(new URL("./fiber-cancellation-workflows.ts", import.meta.url))
describe("activity fiber cancellation", () => {
  it("cancels interrupted typed and legacy calls, preserves successes, and replays", async () => {
    const history = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* makeWorkflowTestHarness({
        workflowsPath,
        activities: {
          ...Typed.implement([Typed.handle(slow, () => Effect.sleep(200).pipe(Effect.as("done")))]),
          ...Runtime.makeActivities(workflow, [legacy])
        }
      })
      return yield* harness.provide(Effect.gen(function*() {
        for (const legacy of [false, true]) {
          for (const mode of ["race", "timeout", "interrupt", "success"] as const) {
            const payload = { mode, legacy }
            expect(yield* workflow.execute(payload)).toBe("done")
            const id = yield* workflow.executionId(payload)
            const events = yield* harness.client.fetchHistoryEvents(`${workflow._tag}/${id}`)
            expect(events.filter((event) => event.activityTaskCancelRequestedEventAttributes), `${legacy}/${mode}`)
              .toHaveLength(mode === "success" ? 0 : 1)
          }
        }
        const id = yield* workflow.executionId({ legacy: false, mode: "race" })
        return yield* harness.client.getHandle(`${workflow._tag}/${id}`).fetchHistory
      }))
    })))
    await expect(Worker.runReplayHistory({ workflowsPath }, history)).resolves.toBeUndefined()
  }, 120_000)
})
