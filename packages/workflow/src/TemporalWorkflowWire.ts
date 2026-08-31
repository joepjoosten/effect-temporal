/**
 * Wire codecs for every value that crosses a Temporal boundary: workflow
 * payloads, workflow results, and durable-deferred exits.
 *
 * Everything on the wire is schema-encoded JSON. Typed workflow failures fail
 * the Temporal run with the full encoded `Workflow.Result` carried in
 * `ApplicationFailure.details[0]` under the {@link workflowExitFailureType}
 * failure type, so every reading side can decode the exit back into the
 * workflow's typed success and error channels.
 *
 * This module is imported inside the workflow sandbox, so it must only depend
 * on `effect` and `@temporalio/common`.
 *
 * @since 1.0.0
 */
import { ApplicationFailure } from "@temporalio/common"
import * as Cause from "effect/Cause"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import * as Workflow from "effect/unstable/workflow/Workflow"

/**
 * The `ApplicationFailure.type` marking a Temporal workflow failure that
 * carries an encoded Effect `Workflow.Result` in `details[0]`.
 *
 * @since 1.0.0
 * @category Constants
 */
export const workflowExitFailureType = "EffectWorkflowExit"

const GenericExitJson = Schema.toCodecJson(
  Schema.Exit(Schema.Any, Schema.Any, Schema.Defect())
)

const encodeGenericExit = Schema.encodeUnknownSync(GenericExitJson)

/**
 * Encodes a durable-deferred exit (an `Exit` whose values are already
 * schema-encoded by `WorkflowEngine.makeUnsafe`) into plain JSON for signal
 * and query payloads. A success of `undefined` is normalized to `null` — JSON
 * cannot carry `undefined` — matching the normalization Effect's engine
 * adapter applies before decoding.
 *
 * @since 1.0.0
 * @category Deferreds
 */
export const encodeDeferredExit = (exit: Exit.Exit<unknown, unknown>): unknown =>
  encodeGenericExit(Exit.map(exit, (value) => value ?? null))

/**
 * Decodes a deferred exit from its JSON wire form back into a real `Exit`
 * instance, with the schema-encoded values inside left untouched.
 *
 * @since 1.0.0
 * @category Deferreds
 */
export const decodeDeferredExit: (encoded: unknown) => Exit.Exit<unknown, unknown> = Schema.decodeUnknownSync(
  GenericExitJson
) as (encoded: unknown) => Exit.Exit<unknown, unknown>

/**
 * Per-workflow codecs for the values crossing the Temporal boundary.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalWorkflowWireCodecs {
  readonly encodePayload: (payload: unknown) => object
  readonly decodePayload: (payload: unknown) => object
  readonly encodeSuccess: (value: unknown) => unknown
  readonly decodeSuccess: (value: unknown) => unknown
  readonly encodeResult: (result: Workflow.Result<unknown, unknown>) => unknown
  readonly decodeResult: (encoded: unknown) => Workflow.Result<unknown, unknown>
}

/**
 * JSON codecs for a single schema'd value crossing a Temporal boundary.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalValueCodecs {
  readonly encode: (value: unknown) => unknown
  readonly decode: (value: unknown) => unknown
}

/**
 * Builds JSON wire codecs for one schema. The sync codecs die loudly for
 * schemas that require decoding services; boundary schemas must be
 * self-contained.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const valueCodecsFor = (schema: Schema.Top): TemporalValueCodecs => {
  const json = Schema.toCodecJson(schema)
  return {
    encode: Schema.encodeUnknownSync(json as any) as (value: unknown) => unknown,
    decode: Schema.decodeUnknownSync(json as any) as (value: unknown) => unknown
  }
}

const codecCache = new WeakMap<object, TemporalWorkflowWireCodecs>()

/**
 * Returns (and caches) the wire codecs derived from a workflow's payload,
 * success, and error schemas.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const wireCodecsFor = (workflow: Workflow.Any): TemporalWorkflowWireCodecs => {
  let codecs = codecCache.get(workflow)
  if (codecs === undefined) {
    const payloadJson = Schema.toCodecJson(workflow.payloadSchema)
    const successJson = Schema.toCodecJson(workflow.successSchema)
    const resultJson = Schema.toCodecJson(
      Workflow.Result({
        success: workflow.successSchema,
        error: workflow.errorSchema
      })
    )
    // The sync codecs die loudly for schemas that require decoding services;
    // workflow boundary schemas must be self-contained.
    codecs = {
      encodePayload: Schema.encodeUnknownSync(payloadJson as any) as (payload: unknown) => object,
      decodePayload: Schema.decodeUnknownSync(payloadJson as any) as (payload: unknown) => object,
      encodeSuccess: Schema.encodeUnknownSync(successJson as any) as (value: unknown) => unknown,
      decodeSuccess: Schema.decodeUnknownSync(successJson as any) as (value: unknown) => unknown,
      encodeResult: Schema.encodeUnknownSync(resultJson as any) as (
        result: Workflow.Result<unknown, unknown>
      ) => unknown,
      decodeResult: Schema.decodeUnknownSync(resultJson as any) as (
        encoded: unknown
      ) => Workflow.Result<unknown, unknown>
    }
    codecCache.set(workflow, codecs)
  }
  return codecs
}

/**
 * Builds the `ApplicationFailure` thrown when an Effect workflow completes
 * with a failing exit. The encoded `Workflow.Result` rides in `details[0]`;
 * the failure is non-retryable because the exit is the workflow's final,
 * typed outcome.
 *
 * @since 1.0.0
 * @category Failures
 */
export const encodeWorkflowFailure = (
  workflow: Workflow.Any,
  result: Workflow.Result<unknown, unknown> & { readonly _tag: "Complete" },
  cause: Cause.Cause<unknown>
): ApplicationFailure =>
  ApplicationFailure.create({
    message: Cause.pretty(cause),
    type: workflowExitFailureType,
    nonRetryable: true,
    details: [wireCodecsFor(workflow).encodeResult(result)]
  })

const isEffectWorkflowExitFailure = (
  error: unknown
): error is ApplicationFailure & { readonly details: ReadonlyArray<unknown> } =>
  error instanceof ApplicationFailure
  && error.type === workflowExitFailureType
  && Array.isArray(error.details)
  && error.details.length > 0

/**
 * Walks a thrown failure's `cause` chain for an `EffectWorkflowExit`
 * `ApplicationFailure` and returns the raw encoded `Workflow.Result` from its
 * details, or `undefined` when the failure was produced outside this
 * protocol.
 *
 * @since 1.0.0
 * @category Failures
 */
export const findEncodedWorkflowExit = (error: unknown): unknown => {
  let current: unknown = error
  while (current !== null && typeof current === "object") {
    if (isEffectWorkflowExitFailure(current)) {
      return current.details[0]
    }
    current = (current as { readonly cause?: unknown }).cause
  }
  return undefined
}

/**
 * Recovers the typed `Workflow.Result` from a thrown Temporal failure, if the
 * failure (or any failure in its `cause` chain) carries an encoded Effect
 * workflow exit. Returns `undefined` for failures produced outside this
 * protocol.
 *
 * @since 1.0.0
 * @category Failures
 */
export const decodeWorkflowFailure = (
  workflow: Workflow.Any,
  error: unknown
): Workflow.Result<unknown, unknown> | undefined => {
  const encoded = findEncodedWorkflowExit(error)
  return encoded === undefined ? undefined : wireCodecsFor(workflow).decodeResult(encoded)
}
