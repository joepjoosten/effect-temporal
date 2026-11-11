import { makeWorkflowTestHarness } from "@effect-temporal/testing/TemporalWorkflowHarness"
import { describe, expect, it } from "@effect/vitest"
import { Worker } from "@temporalio/worker"
import { DeterminismViolationError } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as DurableDeferred from "effect/workflow/DurableDeferred"
import { createHash } from "node:crypto"
import { fileURLToPath } from "node:url"
import * as Interactions from "../src/TemporalWorkflowInteractions.js"
import * as Protocol from "../src/TemporalWorkflowProtocol.js"
import { approval, child, parent, parentDone, prefixedParent, status } from "./child-addressing-workflows.js"

describe("child workflow addressing", { concurrent: false }, () => {
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
  it("derives the pre-rc.118 execution id", async () => {
    const payload = { id: "legacy-digest" }
    const legacy = await Effect.runPromise(Protocol.legacyExecutionId(child, payload))
    expect(legacy).toBe(createHash("sha256").update(`${child._tag}-${payload.id}`).digest("hex").slice(0, 32))
    expect(legacy).not.toBe(await Effect.runPromise(child.executionId(payload)))
  })
  // `recorded-child-addressing-workflows.ts` writes the history of an older
  // release; `discard` selects the pre-rc.118 digest.
  const recordHistory = (payload: { id: string; discard: boolean }) =>
    Effect.runPromise(Effect.scoped(Effect.gen(function*() {
      const harness = yield* makeWorkflowTestHarness({
        workflowsPath: fileURLToPath(new URL("./recorded-child-addressing-workflows.ts", import.meta.url))
      })
      expect(yield* harness.provide(parent.execute(payload))).toBe("approved")
      const id = yield* parent.executionId(payload)
      return yield* harness.client.getHandle(`${parent._tag}/${id}`).fetchHistory
    })))
  const replay = (workflows: string, history: Awaited<ReturnType<typeof recordHistory>>) =>
    Worker.runReplayHistory({ workflowsPath: fileURLToPath(new URL(workflows, import.meta.url)) }, history)
  it("replays pre-marker histories recorded with the current execution id", async () => {
    const history = await recordHistory({ id: "current-digest-child", discard: false })
    await expect(replay("./current-child-addressing-workflows.ts", history)).resolves.toBeUndefined()
  }, 120_000)
  it("replays pre-rc.118 child IDs only with legacyChildExecutionIds", async () => {
    const history = await recordHistory({ id: "legacy-digest-child", discard: true })
    await expect(replay("./legacy-digest-child-addressing-workflows.ts", history)).resolves.toBeUndefined()
    await expect(replay("./current-child-addressing-workflows.ts", history)).rejects.toBeInstanceOf(
      DeterminismViolationError
    )
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
