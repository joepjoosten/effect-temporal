import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import { makeWorkflowTestHarness } from "../src/TemporalWorkflowHarness.js"
import { echoWorkflow } from "./harness-workflows.js"

const workflowsPath = fileURLToPath(new URL("./harness-workflows.ts", import.meta.url))

describe("makeWorkflowTestHarness", () => {
  it("boots environment and worker in one call and runs a workflow end to end", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function*() {
          const harness = yield* makeWorkflowTestHarness({ workflowsPath })
          return yield* harness.provide(echoWorkflow.execute({ message: "hi" }))
        })
      ) as Effect.Effect<string>
    )

    expect(result).toBe("echo:hi")
  }, 120_000)
})
