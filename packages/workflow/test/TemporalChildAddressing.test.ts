import { makeWorkflowTestHarness } from "@effect-temporal/testing/TemporalWorkflowHarness"
import { describe, expect, it } from "@effect/vitest"
import { Worker } from "@temporalio/worker"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import { fileURLToPath } from "node:url"
import * as Interactions from "../src/TemporalWorkflowInteractions.js"
import { approval, child, parent, parentDone, prefixedParent, status } from "./child-addressing-workflows.js"

describe("child workflow addressing", () => {
  it("replays legacy histories without changing their recorded child IDs", async () => {
    const history = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* makeWorkflowTestHarness({
        workflowsPath: fileURLToPath(new URL("./legacy-child-addressing-workflows.ts", import.meta.url))
      })
      const payload = { id: "legacy-child", discard: false }
      expect(yield* harness.provide(parent.execute(payload))).toBe("approved")
      const id = yield* parent.executionId(payload)
      return yield* harness.client.getHandle(`${parent._tag}/${id}`).fetchHistory
    })))
    await expect(Worker.runReplayHistory({
      workflowsPath: fileURLToPath(new URL("./child-addressing-workflows.ts", import.meta.url))
    }, history)).resolves.toBeUndefined()
  }, 120_000)
  it.each([undefined, "custom"])(
    "supports polling, deferreds and attachment with prefix %s",
    async (workflowIdPrefix) => {
      await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
        const harness = yield* makeWorkflowTestHarness({
          workflowsPath: fileURLToPath(new URL("./child-addressing-workflows.ts", import.meta.url)),
          workflowIdPrefix
        })
        yield* harness.provide(Effect.gen(function*() {
          const workflow = workflowIdPrefix === undefined ? parent : prefixedParent
          for (const discard of [false, true]) {
            const payload = { id: `child-${discard}`, discard }
            yield* workflow.execute(payload, { discard: true })
            const executionId = yield* child.executionId(payload)
            let suspended = false
            for (let attempt = 0; attempt < 100; attempt++) {
              const result = yield* child.poll(executionId)
              if (Option.isSome(result) && result.value._tag === "Suspended") {
                suspended = true
                break
              }
              yield* Effect.sleep(50)
            }
            expect(suspended).toBe(true)
            const value = yield* Interactions.readStateCell(status, { workflow: child, executionId, workflowIdPrefix })
            expect(value).toEqual(Option.some("waiting"))
            const token = yield* DurableDeferred.tokenFromPayload(approval, { workflow: child, payload })
            yield* DurableDeferred.succeed(approval, { token, value: "approved" })
            expect(yield* child.execute(payload)).toBe("approved")
            if (discard) {
              const token = yield* DurableDeferred.tokenFromPayload(parentDone, { workflow, payload })
              yield* DurableDeferred.succeed(parentDone, { token, value: "approved" })
            }
            expect(yield* workflow.execute(payload)).toBe("approved")
          }
        }))
      })))
    },
    120_000
  )
})
