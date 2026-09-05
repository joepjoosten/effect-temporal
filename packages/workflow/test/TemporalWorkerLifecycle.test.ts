import * as Testing from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"

const workflowsPath = fileURLToPath(new URL("./TemporalWorker.e2e-workflows.ts", import.meta.url))
describe("worker resource lifecycle", () => {
  it("releases unused, interrupted, completed and failing workers before their connections", async () => {
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const environment = yield* Testing.makeTimeSkipping()
      yield* Effect.gen(function*() {
        const client = yield* Testing.makeClient()
        for (const mode of ["unused", "interrupted", "completed", "failed", "in-flight"] as const) {
          const taskQueue = Testing.makeTaskQueue(`lifecycle-${mode}`)
          const started = yield* Deferred.make<void>()
          const worker = yield* Effect.scoped(Effect.gen(function*() {
            const worker = yield* Testing.makeWorker({
              workflowsPath,
              taskQueue,
              activities: {
                appendActivity: async () => {
                  await Effect.runPromise(Deferred.succeed(started, undefined))
                  await new Promise((resolve) => setTimeout(resolve, 200))
                  return "activity-result"
                }
              }
            })
            if (mode === "completed") {
              expect(yield* worker.runUntil(async () => "done")).toBe("done")
            } else if (mode === "failed") {
              const error = yield* worker.runUntil(async () => {
                throw new Error("inner failure")
              }).pipe(Effect.flip)
              expect(error.cause).toBeInstanceOf(Error)
            } else if (mode !== "unused") {
              yield* Effect.forkScoped(worker.run)
              while (worker.unsafeWorker.getState() === "INITIALIZED") yield* Effect.yieldNow
              if (mode === "in-flight") {
                yield* client.start("EffectTemporalE2EWorkflow", {
                  workflowId: taskQueue,
                  taskQueue,
                  args: [{ message: "hello" }]
                })
                yield* Deferred.await(started)
              }
            }
            return worker
          }))
          expect(worker.unsafeWorker.getState(), mode).toBe("STOPPED")
          // Repeated shutdown must be harmless after scope closure.
          yield* worker.shutdown
        }
      }).pipe(Effect.provideService(Testing.TemporalTestEnvironment, environment))
    })))
  }, 120_000)
})
