/**
 * Schema-typed Nexus operations backed by Effect workflows.
 *
 * A Nexus operation definition pairs a service/operation name with payload,
 * success, and failure schemas, shared by the handler worker and every
 * caller. The workflow-side `call` encodes the payload, invokes the
 * operation through Temporal's Nexus client in its own cancellation scope,
 * and decodes the result — the target workflow's schema'd failure lands in
 * the Effect error channel. The worker-side handler lives in
 * `TemporalNexusService`.
 *
 * This module is sandbox-safe.
 *
 * @since 1.0.0
 */
import { CancellationScope, createNexusServiceClient } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import type * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as NexusRpc from "nexus-rpc"
import { memoizeOutboundCommand, releaseCancellationScope, TemporalSandboxRun } from "./TemporalWorkflowRuntime.js"
import { findEncodedWorkflowExit, type TemporalValueCodecs, valueCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * A named, schema-typed Nexus operation.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalNexusOperation<
  Payload extends Schema.Top = Schema.Top,
  Success extends Schema.Top = Schema.Top,
  Error extends Schema.Top = Schema.Top
> {
  readonly serviceName: string
  readonly operationName: string
  readonly service: NexusRpc.ServiceDefinition<any>
  readonly payloadSchema: Payload
  readonly successSchema: Success
  readonly errorSchema: Error
  readonly payloadCodecs: TemporalValueCodecs
  readonly successCodecs: TemporalValueCodecs
  readonly resultCodecs: TemporalValueCodecs
}

/**
 * Creates a Nexus operation definition, shared by the handler worker and
 * every caller so both sides encode and decode with the same schemas. The
 * success and error schemas must match the backing workflow's.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const make = <
  Payload extends Schema.Top,
  Success extends Schema.Top = typeof Schema.Void,
  Error extends Schema.Top = typeof Schema.Never
>(
  serviceName: string,
  operationName: string,
  options: {
    readonly payload: Payload
    readonly success?: Success
    readonly error?: Error
  }
): TemporalNexusOperation<Payload, Success, Error> => {
  const successSchema = options.success ?? (Schema.Void as unknown as Success)
  const errorSchema = options.error ?? (Schema.Never as unknown as Error)
  return {
    errorSchema,
    operationName,
    payloadCodecs: valueCodecsFor(options.payload),
    payloadSchema: options.payload,
    resultCodecs: valueCodecsFor(
      Workflow.Result({
        error: errorSchema as any,
        success: successSchema as any
      }) as unknown as Schema.Top
    ),
    service: NexusRpc.service(serviceName, {
      [operationName]: NexusRpc.operation<unknown, unknown>({ name: operationName })
    }),
    serviceName,
    successCodecs: valueCodecsFor(successSchema),
    successSchema
  }
}

/**
 * Calls the Nexus operation from a workflow body. The payload is encoded
 * through the operation's schema; the call runs in its own cancellation
 * scope so interrupts and cancellations propagate to the operation; the
 * backing workflow's typed failure is decoded into the Effect error channel.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const call = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top
>(
  operation: TemporalNexusOperation<Payload, Success, Error>,
  target: {
    readonly endpoint: string
  },
  payload: Payload["Type"]
): Effect.Effect<Success["Type"], Error["Type"], TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    const client = createNexusServiceClient({
      endpoint: target.endpoint,
      service: operation.service
    })
    const scope = new CancellationScope(
      state.runScope === undefined
        ? undefined
        : { cancellable: true, parent: state.runScope }
    )
    state.inFlight.add(scope)
    const outcome = yield* Effect.tryPromise({
      try: () =>
        memoizeOutboundCommand(
          state,
          JSON.stringify(["nexus", target.endpoint, operation.serviceName, operation.operationName]),
          () =>
            scope.run(() =>
              client.executeOperation(
                operation.operationName as never,
                operation.payloadCodecs.encode(payload) as never
              )
            )
        ),
      catch: (cause) => cause
    }).pipe(
      Effect.onExit((exit) => Effect.sync(() => releaseCancellationScope(state, scope, exit))),
      Effect.exit
    )

    if (outcome._tag === "Success") {
      return operation.successCodecs.decode(outcome.value) as Success["Type"]
    }
    const failure = outcome.cause.reasons.find((reason) => reason._tag === "Fail")
    const thrown = failure !== undefined && "error" in failure ? failure.error : undefined
    const encodedExit = findEncodedWorkflowExit(thrown)
    if (encodedExit !== undefined) {
      const result = operation.resultCodecs.decode(encodedExit) as Workflow.Result<
        Success["Type"],
        Error["Type"]
      >
      if (result._tag === "Complete") {
        return yield* result.exit as Exit.Exit<Success["Type"], Error["Type"]>
      }
    }
    return yield* Effect.die(thrown ?? outcome.cause)
  })
