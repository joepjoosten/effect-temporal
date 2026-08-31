/**
 * Schema-typed activities with explicit payloads.
 *
 * Effect's `Activity.make` couples an activity to an effect closed over
 * workflow-body state — closures that cannot cross the worker boundary on
 * Temporal, where activities execute in a separate Node process. A typed
 * activity instead declares an explicit payload schema alongside success and
 * failure schemas and per-activity Temporal options; the definition is shared
 * by the workflow bundle and the worker so the two sides cannot drift.
 *
 * - Workflow side (`call`): encodes the payload, schedules a real Temporal
 *   activity, and decodes the typed exit — the schema'd failure lands in the
 *   Effect error channel.
 * - Worker side (`handle` + `implement`): binds an implementation
 *   `(payload) => Effect` to the definition. Typed failures complete the
 *   Temporal activity successfully carrying the encoded exit (no Temporal
 *   retry); defects fail the activity so Temporal's retry policy applies.
 *
 * `make` and `call` are sandbox-safe; `handle`/`implement` run on the worker.
 *
 * @since 1.0.0
 */
import type { ActivityOptions } from "@temporalio/common"
import { CancellationScope, proxyActivities, proxyLocalActivities } from "@temporalio/workflow"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"
import * as Exit from "effect/Exit"
import * as Schema from "effect/Schema"
import { TemporalSandboxRun, type TemporalWorkflowRuntimeState } from "./TemporalWorkflowRuntime.js"
import { type TemporalValueCodecs, valueCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * Per-activity Temporal options, declared with the definition.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalTypedActivityOptions extends
  Pick<
    ActivityOptions,
    "startToCloseTimeout" | "scheduleToCloseTimeout" | "scheduleToStartTimeout" | "heartbeatTimeout" | "retry"
  >
{
  /**
   * Schedule as a Temporal local activity.
   */
  readonly local?: boolean | undefined
}

/**
 * A named activity definition with payload, success, and failure schemas.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalTypedActivity<
  Payload extends Schema.Top = Schema.Top,
  Success extends Schema.Top = Schema.Top,
  Error extends Schema.Top = Schema.Top
> {
  readonly name: string
  readonly payloadSchema: Payload
  readonly successSchema: Success
  readonly errorSchema: Error
  readonly options: TemporalTypedActivityOptions
  readonly payloadCodecs: TemporalValueCodecs
  readonly exitCodecs: TemporalValueCodecs
}

const defaultOptions = {
  startToCloseTimeout: "10 minutes"
} satisfies TemporalTypedActivityOptions

/**
 * Creates a typed activity definition, shared by the workflow bundle and the
 * worker.
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
    readonly options?: TemporalTypedActivityOptions | undefined
  }
): TemporalTypedActivity<Payload, Success, Error> => {
  const successSchema = options.success ?? (Schema.Void as unknown as Success)
  const errorSchema = options.error ?? (Schema.Never as unknown as Error)
  return {
    name,
    payloadSchema: options.payload,
    successSchema,
    errorSchema,
    options: options.options ?? defaultOptions,
    payloadCodecs: valueCodecsFor(options.payload),
    exitCodecs: valueCodecsFor(
      Schema.Exit(successSchema as any, errorSchema as any, Schema.Defect()) as unknown as Schema.Top
    )
  }
}

type BridgeProxy = Record<string, (payload: unknown) => Promise<unknown>>

const proxyCache = new WeakMap<TemporalTypedActivity<any, any, any>, BridgeProxy>()

const proxyFor = (activity: TemporalTypedActivity<any, any, any>): BridgeProxy => {
  let proxy = proxyCache.get(activity)
  if (proxy === undefined) {
    const { local, ...activityOptions } = activity.options
    const options = {
      ...activityOptions,
      startToCloseTimeout: activityOptions.startToCloseTimeout
        ?? (activityOptions.scheduleToCloseTimeout === undefined ? defaultOptions.startToCloseTimeout : undefined)
    }
    proxy = local === true
      ? proxyLocalActivities<BridgeProxy>(options as any)
      : proxyActivities<BridgeProxy>(options as any)
    proxyCache.set(activity, proxy)
  }
  return proxy
}

const invokeOnce = (
  state: TemporalWorkflowRuntimeState,
  activity: TemporalTypedActivity<any, any, any>,
  encodedPayload: unknown
): Effect.Effect<unknown> =>
  Effect.suspend(() => {
    const scope = new CancellationScope(
      state.runScope === undefined
        ? undefined
        : { cancellable: true, parent: state.runScope }
    )
    state.inFlight.add(scope)
    return Effect.tryPromise({
      try: () => scope.run(() => proxyFor(activity)[activity.name](encodedPayload)),
      catch: (cause) => new Error(`Typed activity "${activity.name}" failed`, { cause })
    }).pipe(
      Effect.orDie,
      Effect.onExit(() => Effect.sync(() => state.inFlight.delete(scope)))
    )
  })

/**
 * Calls the typed activity from a workflow body: the payload is encoded and a
 * real Temporal activity is scheduled; the typed exit is decoded back so the
 * schema'd failure lands in the Effect error channel. Completed exits are
 * memoized per call position, so a resumed pass of the body replays results
 * instead of re-scheduling.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const call = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top
>(
  activity: TemporalTypedActivity<Payload, Success, Error>,
  payload: Payload["Type"]
): Effect.Effect<Success["Type"], Error["Type"], TemporalSandboxRun> =>
  Effect.gen(function*() {
    const state = yield* TemporalSandboxRun
    const sequence = state.typedActivitySeq.get(activity.name) ?? 0
    state.typedActivitySeq.set(activity.name, sequence + 1)
    const memoKey = `${activity.name}/${sequence}`
    let encodedExit = state.typedActivityResults.get(memoKey)
    if (encodedExit === undefined) {
      encodedExit = yield* invokeOnce(state, activity, activity.payloadCodecs.encode(payload))
      state.typedActivityResults.set(memoKey, encodedExit)
    }
    const exit = activity.exitCodecs.decode(encodedExit) as Exit.Exit<Success["Type"], Error["Type"]>
    return yield* exit
  })

/**
 * A typed activity bound to its worker-side implementation.
 *
 * @since 1.0.0
 * @category Models
 */
export interface BoundTypedActivity {
  readonly activity: TemporalTypedActivity<any, any, any>
  readonly handler: (payload: any) => Effect.Effect<any, any, any>
}

/**
 * Binds an implementation to a typed activity definition.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const handle = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  R = never
>(
  activity: TemporalTypedActivity<Payload, Success, Error>,
  handler: (payload: Payload["Type"]) => Effect.Effect<Success["Type"], Error["Type"], R>
): BoundTypedActivity => ({ activity, handler })

/**
 * Builds the worker `activities` record for a set of bound typed activities.
 * Typed failures are encoded into the activity result (Temporal does not
 * retry them); defects and interruptions fail the Temporal activity, so its
 * retry policy applies.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const implement = (
  bound: ReadonlyArray<BoundTypedActivity>,
  options?: {
    readonly provide?: (<A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, never>) | undefined
  } | undefined
): Record<string, (payload: unknown) => Promise<unknown>> =>
  Object.fromEntries(
    bound.map(({ activity, handler }) => [
      activity.name,
      async (payload: unknown) => {
        const base = Effect.suspend(() => handler(activity.payloadCodecs.decode(payload)))
        const program = options?.provide === undefined ? base : options.provide(base)
        const exit = await Effect.runPromise(Effect.exit(program as Effect.Effect<unknown, unknown>))
        if (Exit.isFailure(exit) && !Cause.hasFails(exit.cause)) {
          throw Cause.squash(exit.cause)
        }
        return activity.exitCodecs.encode(exit)
      }
    ])
  )
