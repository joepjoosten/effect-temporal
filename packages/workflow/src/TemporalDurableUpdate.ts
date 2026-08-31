/**
 * Typed request/response interactions with running workflows.
 *
 * A durable update is a named channel of schema-typed requests, each of which
 * the workflow answers with a typed success or a typed failure. Requests
 * arrive through a reserved signal into a per-run log; the workflow body
 * serves them with `take`, and each request carries a one-shot typed
 * `respond` capability whose exit is recorded per request id and served by
 * the reserved update-result query. Clients call
 * `TemporalWorkflowInteractions.executeUpdate` to send a request and await
 * the typed response.
 *
 * This module is sandbox-safe.
 *
 * @since 1.0.0
 */
import { condition } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import type { UpdateSignal } from "./TemporalWorkflowProtocol.js"
import { TemporalSandboxRun } from "./TemporalWorkflowRuntime.js"
import { type TemporalValueCodecs, valueCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * A named, schema-typed durable update channel.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalDurableUpdate<
  Payload extends Schema.Top = Schema.Top,
  Success extends Schema.Top = Schema.Top,
  Error extends Schema.Top = Schema.Top
> {
  readonly name: string
  readonly payloadSchema: Payload
  readonly successSchema: Success
  readonly errorSchema: Error
  readonly payloadCodecs: TemporalValueCodecs
  readonly exitCodecs: TemporalValueCodecs
}

/**
 * Creates an update definition, shared by the workflow bundle and every
 * client. Both the success and the failure channel are schemas, so callers
 * receive typed errors, not just typed successes.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top = typeof Schema.Void,
  Error extends Schema.Top = typeof Schema.Never
>(
  name: string,
  options: {
    readonly payload: Payload
    readonly success?: Success
    readonly error?: Error
  }
): TemporalDurableUpdate<Payload, Success, Error> => {
  const successSchema = options.success ?? (Schema.Void as unknown as Success)
  const errorSchema = options.error ?? (Schema.Never as unknown as Error)
  return {
    name,
    payloadSchema: options.payload,
    successSchema,
    errorSchema,
    payloadCodecs: valueCodecsFor(options.payload),
    exitCodecs: valueCodecsFor(
      Schema.Exit(successSchema as any, errorSchema as any, Schema.Defect()) as unknown as Schema.Top
    )
  }
}

/**
 * One pending update request, with its decoded payload and a one-shot typed
 * response capability.
 *
 * @since 1.0.0
 * @category Models
 */
export interface UpdateRequest<
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top
> {
  readonly requestId: string
  readonly payload: Payload["Type"]
  readonly respond: (exit: Exit.Exit<Success["Type"], Error["Type"]>) => Effect.Effect<void>
  readonly succeed: (value: Success["Type"]) => Effect.Effect<void>
  readonly fail: (error: Error["Type"]) => Effect.Effect<void>
}

/**
 * Takes the next update request, durably waiting until one arrives. Waiting
 * blocks on a Temporal condition — the run stays open. The returned request
 * must be answered through its `respond` / `succeed` / `fail` capability; the
 * recorded exit is what the caller of `executeUpdate` receives.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const take = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top
>(
  update: TemporalDurableUpdate<Payload, Success, Error>
): Effect.Effect<UpdateRequest<Payload, Success, Error>, never, TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    let request: UpdateSignal
    while (true) {
      const log = state.updateLogs.get(update.name)
      const cursor = state.updateCursors.get(update.name) ?? 0
      if (log !== undefined && cursor < log.length) {
        state.updateCursors.set(update.name, cursor + 1)
        request = log[cursor]
        break
      }
      yield* Effect.promise(() => condition(() => (state.updateLogs.get(update.name)?.length ?? 0) > cursor))
    }
    const respond = (exit: Exit.Exit<Success["Type"], Error["Type"]>): Effect.Effect<void> =>
      Effect.sync(() => {
        state.updateResults.set(request.requestId, update.exitCodecs.encode(exit))
      })
    return {
      requestId: request.requestId,
      payload: update.payloadCodecs.decode(request.payload) as Payload["Type"],
      respond,
      succeed: (value) => respond(Exit.succeed(value)),
      fail: (error) => respond(Exit.fail(error))
    }
  })
