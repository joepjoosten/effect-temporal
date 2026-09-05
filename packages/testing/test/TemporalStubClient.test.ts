import { makeStubTemporalClient } from "@effect-temporal/testing"
import * as TemporalWorkflowEngine from "@effect-temporal/workflow/TemporalWorkflowEngine"
import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"

const stubWorkflow = Workflow.make("StubTestWorkflow", {
  payload: {
    orderId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ orderId }) => orderId
})

const runWithStub = <A, E>(
  stub: ReturnType<typeof makeStubTemporalClient>,
  effect: Effect.Effect<A, E, WorkflowEngine.WorkflowEngine>
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function*() {
      const engine = yield* TemporalWorkflowEngine.make({ taskQueue: "stub-queue" })
      return yield* effect.pipe(Effect.provideService(WorkflowEngine.WorkflowEngine, engine))
    }).pipe(Effect.provide(stub.layer))
  )

describe("makeStubTemporalClient", () => {
  it("drives the real client-side engine with zero servers, recording starts", async () => {
    const stub = makeStubTemporalClient({
      resultFor: () => "recorded-result"
    })

    const result = await runWithStub(stub, stubWorkflow.execute({ orderId: "ord-1" }))

    expect(result).toBe("recorded-result")
    expect(stub.recorded.starts).toHaveLength(1)
    expect(stub.recorded.starts[0].workflow).toBe("StubTestWorkflow")
    expect(stub.recorded.starts[0].workflowId.startsWith("StubTestWorkflow/")).toBe(true)
    // The engine encodes the payload before handing it to the client.
    expect((stub.recorded.starts[0].options as { args: ReadonlyArray<unknown> }).args).toEqual([
      { orderId: "ord-1" }
    ])
  })

  it("simulates already-started executions so the engine attaches to the result", async () => {
    const probe = makeStubTemporalClient()
    // Learn the workflow id the engine derives for this payload.
    await runWithStub(
      probe,
      stubWorkflow.execute({ orderId: "ord-2" }).pipe(Effect.exit)
    ).catch(() => undefined)
    const workflowId = probe.recorded.starts[0]?.workflowId

    const stub = makeStubTemporalClient({
      alreadyStartedWorkflowIds: [workflowId],
      resultFor: (id) => (id === workflowId ? "attached-result" : "wrong")
    })

    const result = await runWithStub(stub, stubWorkflow.execute({ orderId: "ord-2" }))

    expect(result).toBe("attached-result")
    expect(stub.recorded.starts).toHaveLength(1)
  })

  it("records signals and terminations through handles", async () => {
    const stub = makeStubTemporalClient()

    await Effect.runPromise(
      Effect.gen(function*() {
        const handle = stub.client.getHandle("wf-1")
        yield* handle.signal("my-signal", { hello: "world" })
        yield* handle.terminate("cleanup")
      })
    )

    expect(stub.recorded.signals).toEqual([
      { args: [{ hello: "world" }], signalName: "my-signal", workflowId: "wf-1" }
    ])
    expect(stub.recorded.terminations).toEqual([{ reason: "cleanup", workflowId: "wf-1" }])
  })

  it("throws a descriptive error for unconfigured surface members", () => {
    const stub = makeStubTemporalClient()
    expect(() => (stub.client as { list: unknown }).list).toThrow(
      "Stub Temporal client: \"StubTemporalClient.list\" is not configured"
    )
  })
})
