import { makeWorkflowTestHarness } from "@effect-temporal/testing/TemporalWorkflowHarness"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import { fileURLToPath } from "node:url"
import type * as TemporalDurableUpdate from "../src/TemporalDurableUpdate.js"
import * as Interactions from "../src/TemporalWorkflowInteractions.js"
import { updateSignalName, workflowIdFor } from "../src/TemporalWorkflowProtocol.js"
import { counter, increment } from "./update-idempotency-workflows.js"

describe("durable update idempotency", () => {
  it("deduplicates pending and completed retries, rejects conflicts, and recovers after closure", async () => {
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* makeWorkflowTestHarness({
        workflowsPath: fileURLToPath(new URL("./update-idempotency-workflows.ts", import.meta.url))
      })
      yield* harness.provide(Effect.gen(function*() {
        const payload = { id: "counter" }
        const target = { workflow: counter, executionId: yield* counter.executionId(payload) }
        yield* counter.execute(payload, { discard: true })
        const send = (requestId: string, amount: number, timeoutMillis = 5000) =>
          Interactions.executeUpdate(increment, target, amount, { requestId, timeoutMillis, pollIntervalMillis: 5 })
        const timeout = yield* send("first", 5, 10).pipe(Effect.flip)
        expect(timeout._tag).toBe("UpdateTimeoutError")
        // Exercise server-side deduplication, including duplicate signals already in flight.
        const handle = harness.client.getHandle(workflowIdFor(counter._tag, target.executionId))
        for (let duplicate = 0; duplicate < 2; duplicate++) {
          yield* handle.signal(updateSignalName, { name: increment.name, requestId: "first", payload: 5 })
        }
        const retries = yield* Effect.all([send("first", 5), send("first", 5)], { concurrency: "unbounded" })
        expect(retries).toEqual([5, 5])
        expect((yield* send("first", 99).pipe(Effect.flip))._tag).toBe("UpdateConflictError")
        const anotherChannel = {
          ...increment,
          name: "another-channel"
        } satisfies TemporalDurableUpdate.TemporalDurableUpdate
        const conflict = yield* Interactions.executeUpdate(anotherChannel, target, 5, { requestId: "first" }).pipe(
          Effect.flip
        )
        expect(conflict._tag).toBe("UpdateConflictError")
        expect(yield* send("second", 3)).toBe(8)
        expect(yield* send("third", 4)).toBe(12)
        expect(yield* counter.execute(payload)).toBe(12)
        expect(yield* send("first", 5)).toBe(5)
      }))
    })))
  }, 120_000)
})
