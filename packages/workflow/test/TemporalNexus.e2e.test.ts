import * as TemporalTesting from "@effect-temporal/testing"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import { fileURLToPath } from "node:url"
import * as TemporalNexusService from "../src/TemporalNexusService.js"
import { approval, purchaserWorkflow, quoteOperation, quoteWorkflow } from "./TemporalNexus.e2e-workflows.js"

const workflowsPath = fileURLToPath(new URL("./TemporalNexus.e2e-workflows.ts", import.meta.url))

describe("TemporalNexus e2e", () => {
  it("calls a typed Nexus operation backed by an Effect workflow, with typed failures", async () => {
    const endpointName = `effect-nexus-${Date.now()}`

    const result = await Effect.runPromise(Effect.scoped(
      Effect.gen(function*() {
        // Nexus requires the full local dev server (not the time-skipping one).
        const environment = yield* TemporalTesting.makeLocal({
          server: {
            extraArgs: ["--dynamic-config-value", "system.enableNexus=true"]
          }
        })
        const taskQueue = TemporalTesting.makeTaskQueue("effect-nexus-e2e")

        yield* Effect.tryPromise(() =>
          environment.unsafeEnvironment.connection.operatorService.createNexusEndpoint({
            spec: {
              name: endpointName,
              target: {
                worker: {
                  namespace: environment.namespace ?? "default",
                  taskQueue
                }
              }
            }
          })
        )

        const executeProgram = Effect.gen(function*() {
          const payload = { endpoint: endpointName, sku: "sku-1" }
          const executionId = yield* purchaserWorkflow.executionId(payload)
          yield* purchaserWorkflow.execute(payload, { discard: true })
          let suspended = false
          for (let attempt = 0; attempt < 200; attempt++) {
            const result = yield* purchaserWorkflow.poll(executionId)
            if (Option.isSome(result) && result.value._tag === "Suspended") {
              suspended = true
              break
            }
            yield* Effect.sleep(50)
          }
          expect(suspended).toBe(true)
          const token = yield* DurableDeferred.tokenFromPayload(approval, { workflow: purchaserWorkflow, payload })
          yield* DurableDeferred.succeed(approval, { token, value: "resume" })
          const result = yield* purchaserWorkflow.execute(payload)
          const client = yield* TemporalTesting.makeClient()
          const events = yield* client.fetchHistoryEvents(`${purchaserWorkflow._tag}/${executionId}`)
          // One successful and one typed-failing command; neither repeats after resume.
          expect(events.filter((event) => event.nexusOperationScheduledEventAttributes)).toHaveLength(2)
          return result
        }).pipe(
          Effect.provide(TemporalTesting.workflowEngineLayer({ taskQueue })),
          Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment),
          Effect.scoped
        )

        return yield* TemporalTesting.runWorkerUntil(
          {
            nexusServices: [TemporalNexusService.workflowRunOperation(quoteOperation, quoteWorkflow)],
            taskQueue,
            workflowsPath
          },
          () => Effect.runPromise(executeProgram as Effect.Effect<string>)
        ).pipe(
          Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment)
        )
      })
    ))

    expect(result).toBe("300:rejected(invalid quantity 0)")
  }, 240_000)
})
