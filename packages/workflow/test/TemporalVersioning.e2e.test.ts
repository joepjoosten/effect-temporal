import * as TemporalTesting from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import { Worker } from "@temporalio/worker"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import { versionedWorkflow } from "./versioning-gen1-workflows.js"

const gen1Path = fileURLToPath(new URL("./versioning-gen1-workflows.ts", import.meta.url))
const gen2Path = fileURLToPath(new URL("./versioning-gen2-workflows.ts", import.meta.url))
const gen3BrokenPath = fileURLToPath(new URL("./versioning-gen3-broken-workflows.ts", import.meta.url))

const runGeneration = (
  workflowsPath: string,
  payload: { readonly runId: string }
): Promise<{ history: unknown; result: string }> =>
  Effect.runPromise(Effect.scoped(
    Effect.gen(function*() {
      const environment = yield* TemporalTesting.makeTimeSkipping()
      const taskQueue = TemporalTesting.makeTaskQueue("effect-versioning-e2e")
      const executeProgram = Effect.gen(function*() {
        const result = yield* versionedWorkflow.execute(payload)
        const executionId = yield* versionedWorkflow.executionId(payload)
        const client = yield* TemporalTesting.makeClient()
        const history = yield* client.getHandle(`${versionedWorkflow._tag}/${executionId}`).fetchHistory
        return { history, result }
      }).pipe(
        Effect.provide(TemporalTesting.workflowEngineLayer({ taskQueue })),
        Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment),
        Effect.scoped
      )
      return yield* TemporalTesting.runWorkerUntil(
        { taskQueue, workflowsPath },
        () => Effect.runPromise(executeProgram as Effect.Effect<{ history: unknown; result: string }>)
      ).pipe(
        Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment)
      )
    })
  ))

describe("TemporalVersioning e2e", () => {
  it("new executions take the newest version; old histories replay their recorded branch", async () => {
    // Generation 1 records the v1 marker and takes its branch.
    const gen1 = await runGeneration(gen1Path, { runId: "gen1-run" })
    expect(gen1.result).toBe("v1-result")

    // A fresh execution on generation 2 takes the newest version.
    const gen2 = await runGeneration(gen2Path, { runId: "gen2-run" })
    expect(gen2.result).toBe("v2-result")

    // The generation-1 history replays cleanly against generation-2 code:
    // match follows the recorded v1 marker, preserving its commands.
    await expect(
      Worker.runReplayHistory({ workflowsPath: gen2Path }, gen1.history)
    ).resolves.toBeUndefined()

    // Negative control: code that changed the v1 branch's commands without a
    // marker is rejected as non-deterministic.
    await expect(
      Worker.runReplayHistory({ workflowsPath: gen3BrokenPath }, gen1.history)
    ).rejects.toThrow()
  }, 240_000)
})
