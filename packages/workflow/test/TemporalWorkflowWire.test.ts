import { describe, expect, it } from "@effect/vitest"
import * as Cause from "effect/Cause"
import * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"
import {
  decodeDeferredExit,
  decodeWorkflowFailure,
  encodeDeferredExit,
  encodeWorkflowFailure,
  wireCodecsFor,
  workflowExitFailureType
} from "../src/TemporalWorkflowWire.js"

class PaymentDeclined extends Schema.TaggedError<PaymentDeclined>()("PaymentDeclined", {
  code: Schema.Number
}) {}

const workflow = Workflow.make("WireTestWorkflow", {
  payload: {
    orderId: Schema.String,
    amount: Schema.Number
  },
  success: Schema.Struct({ chargeId: Schema.String }),
  error: PaymentDeclined,
  idempotencyKey: ({ orderId }) => orderId
})

const roundTripJson = (value: unknown): unknown => JSON.parse(JSON.stringify(value))

describe("TemporalWorkflowWire", () => {
  it("round-trips payloads through JSON", () => {
    const codecs = wireCodecsFor(workflow)
    const payload = { orderId: "ord-1", amount: 42 }
    expect(codecs.decodePayload(roundTripJson(codecs.encodePayload(payload)))).toEqual(payload)
  })

  it("round-trips successful results through JSON", () => {
    const codecs = wireCodecsFor(workflow)
    const result = new Workflow.Complete({ exit: Exit.succeed({ chargeId: "ch-1" }) })
    const decoded = codecs.decodeResult(roundTripJson(codecs.encodeResult(result)))
    expect(decoded._tag).toBe("Complete")
    expect((decoded as Workflow.Complete<unknown, unknown>).exit).toEqual(Exit.succeed({ chargeId: "ch-1" }))
  })

  it("round-trips typed failures through the ApplicationFailure wire format", () => {
    const error = new PaymentDeclined({ code: 51 })
    const result = new Workflow.Complete({ exit: Exit.fail(error) })
    const failure = encodeWorkflowFailure(workflow, result, Cause.fail(error))

    expect(failure.type).toBe(workflowExitFailureType)
    expect(failure.nonRetryable).toBe(true)

    // Simulate the Temporal failure converter round-tripping details as JSON,
    // nested under an outer failure the way WorkflowFailedError delivers it.
    const rehydrated = Object.assign(
      encodeWorkflowFailure(workflow, result, Cause.fail(error)),
      { details: roundTripJson(failure.details) }
    )
    const outer = new Error("Workflow execution failed")
    ;(outer as Error & { cause: unknown }).cause = rehydrated

    const decoded = decodeWorkflowFailure(workflow, outer)
    expect(decoded?._tag).toBe("Complete")
    const exit = (decoded as Workflow.Complete<unknown, unknown>).exit
    expect(Exit.isFailure(exit)).toBe(true)
    const decodedError = Option.getOrThrow(
      Cause.findErrorOption((exit as Exit.Failure<unknown, unknown>).cause)
    )
    expect(decodedError).toBeInstanceOf(PaymentDeclined)
    expect((decodedError as PaymentDeclined).code).toBe(51)
  })

  it("returns undefined for failures outside the protocol", () => {
    expect(decodeWorkflowFailure(workflow, new Error("boom"))).toBeUndefined()
    expect(decodeWorkflowFailure(workflow, undefined)).toBeUndefined()
  })

  it("round-trips deferred exits through JSON", () => {
    for (const exit of [Exit.succeed({ approverId: "boss" }), Exit.fail("denied")]) {
      const decoded = decodeDeferredExit(roundTripJson(encodeDeferredExit(exit)))
      expect(decoded).toEqual(exit)
    }
    // JSON cannot carry undefined; a void success is normalized to null.
    expect(decodeDeferredExit(roundTripJson(encodeDeferredExit(Exit.void)))).toEqual(Exit.succeed(null))
  })

  it("round-trips suspended results including their absence of cause", () => {
    const codecs = wireCodecsFor(workflow)
    const suspended = new Workflow.Suspended({ cause: undefined })
    const decoded = codecs.decodeResult(roundTripJson(codecs.encodeResult(suspended)))
    expect(decoded._tag).toBe("Suspended")
  })
})
