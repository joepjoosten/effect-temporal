import { makeWorkflowTestHarness } from "@effect-temporal/testing/TemporalWorkflowHarness"
import { describe, expect, it } from "@effect/vitest"
import { Worker } from "@temporalio/worker"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import { fileURLToPath } from "node:url"
import { approval, receiver, sender } from "./outbound-resume-workflows.js"

const workflowsPath = fileURLToPath(new URL("./outbound-resume-workflows.ts", import.meta.url))
describe("outbound commands after suspension", () => {
  it("delivers each logical offer once and replays its history", async () => {
    const history = await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* makeWorkflowTestHarness({ workflowsPath })
      return yield* harness.provide(Effect.gen(function*() {
        const payload = { id: "messages" }
        const executionId = yield* sender.executionId(payload)
        yield* receiver.execute(payload, { discard: true })
        yield* sender.execute(payload, { discard: true })
        let suspended = false
        for (let attempt = 0; attempt < 100; attempt++) {
          const result = yield* sender.poll(executionId)
          if (Option.isSome(result) && result.value._tag === "Suspended") {
            suspended = true
            break
          }
          yield* Effect.sleep(50)
        }
        expect(suspended).toBe(true)
        const token = yield* DurableDeferred.tokenFromPayload(approval, { workflow: sender, payload })
        yield* DurableDeferred.succeed(approval, { token, value: "resume" })
        expect(yield* sender.execute(payload)).toBe("sent")
        expect(yield* receiver.execute(payload)).toBe("first,second,after")
        const history = yield* harness.client.getHandle(`${sender._tag}/${executionId}`).fetchHistory
        expect(history.events?.filter((event) => event.signalExternalWorkflowExecutionInitiatedEventAttributes))
          .toHaveLength(3)
        return history
      }))
    })))
    await expect(Worker.runReplayHistory({ workflowsPath }, history)).resolves.toBeUndefined()
  }, 120_000)
})
