import * as DataConverter from "@effect-temporal/client/TemporalDataConverter"
import * as Testing from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import { echoWorkflow, timerWorkflow } from "./harness-workflows.js"

const workflowsPath = fileURLToPath(new URL("./harness-workflows.ts", import.meta.url))

describe("makeWorkflowTestHarness", () => {
  it("skips one-day timers through the engine and clients with custom converters", async () => {
    let decoded = 0
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* Testing.makeWorkflowTestHarness({ workflowsPath })
      const result = yield* harness.provide(timerWorkflow.execute({ id: "engine" })).pipe(Effect.timeout(10_000))
      expect(result).toBe("done")
      const client = yield* harness.provide(Testing.makeClientWithDataConverter({ identity: "custom-test-client" }))
        .pipe(
          Effect.provideService(DataConverter.TemporalDataConverter, {
            payloadCodecs: [{
              encode: async (payloads) => payloads,
              decode: async (payloads) => {
                decoded++
                return payloads
              }
            }]
          })
        )
      const converted = yield* client.execute("HarnessTimerWorkflow", {
        workflowId: "converted-timer",
        taskQueue: harness.taskQueue,
        args: [{ id: "converted" }]
      }).pipe(Effect.timeout(10_000))
      expect(converted).toBe("done")
      expect(decoded).toBeGreaterThan(0)
      expect(client.unsafeClient.options.identity).toBe("custom-test-client")
    })))
  }, 120_000)
  it("boots environment and worker in one call and runs a workflow end to end", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function*() {
          const harness = yield* Testing.makeWorkflowTestHarness({ workflowsPath })
          return yield* harness.provide(echoWorkflow.execute({ message: "hi" }))
        })
      )
    )

    expect(result).toBe("echo:hi")
  }, 120_000)
})
