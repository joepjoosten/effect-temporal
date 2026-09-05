import { makeWorkflowTestHarness } from "@effect-temporal/testing/TemporalWorkflowHarness"
import { describe, expect, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import { fileURLToPath } from "node:url"
import { sender, target } from "./target-routing-workflows.js"

describe("sandbox target routing", () => {
  it("routes completions, controls and clocks without changing the sender", async () => {
    await Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* makeWorkflowTestHarness({
        workflowsPath: fileURLToPath(new URL("./target-routing-workflows.ts", import.meta.url)),
        workflowIdPrefix: "custom"
      })
      yield* harness.provide(Effect.gen(function*() {
        for (const mode of ["complete", "interrupt", "interruptUnsafe", "resume", "clock"] as const) {
          const payload = { id: mode, mode }
          const executionId = yield* target.executionId(payload)
          yield* target.execute(payload, { discard: true })
          let suspended = false
          for (let attempt = 0; attempt < 100; attempt++) {
            const result = yield* target.poll(executionId)
            if (Option.isSome(result) && result.value._tag === "Suspended") {
              suspended = true
              break
            }
            yield* Effect.sleep(50)
          }
          expect(suspended, mode).toBe(true)
          expect(yield* sender.execute(payload), mode).toBe("sent")
          const result = yield* target.execute(payload).pipe(Effect.timeout(5000), Effect.exit)
          if (mode === "interrupt" || mode === "interruptUnsafe") {
            expect(Exit.isFailure(result), mode).toBe(true)
            if (Exit.isFailure(result)) expect(Cause.hasInterruptsOnly(result.cause), mode).toBe(true)
          } else {
            expect(result).toEqual(Exit.succeed(mode === "clock" ? "woke" : mode === "resume" ? "resumed" : "approved"))
          }
        }
      }))
    })))
  }, 120_000)
})
