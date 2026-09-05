/**
 * @since 1.0.0
 */
import * as TemporalDataConverter from "@effect-temporal/client/TemporalDataConverter"
import { NativeConnection, type NativeConnectionOptions, Worker, type WorkerOptions } from "@temporalio/worker"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import { TemporalWorkerError } from "./TemporalError.js"

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalWorkerConnection = NativeConnection

/**
 * @since 1.0.0
 * @category Tags
 */
export const TemporalWorkerConnection = Context.Service<TemporalWorkerConnection>(
  "@effect-temporal/workflow/TemporalWorkerConnection"
)

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalWorkerConnectionConfig = NativeConnectionOptions

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalWorker {
  readonly unsafeWorker: Worker
  readonly run: Effect.Effect<void, TemporalWorkerError>
  readonly runUntil: <A>(thunk: () => Promise<A>) => Effect.Effect<A, TemporalWorkerError>
  readonly shutdown: Effect.Effect<void>
}

/**
 * @since 1.0.0
 * @category Tags
 */
export const TemporalWorker = Context.Service<TemporalWorker>(
  "@effect-temporal/workflow/TemporalWorker"
)

const wrap = <A>(
  message: string,
  thunk: () => Promise<A>
): Effect.Effect<A, TemporalWorkerError> =>
  Effect.tryPromise({
    try: thunk,
    catch: (cause) =>
      new TemporalWorkerError({
        message,
        cause
      })
  })

const ownWorker = (worker: Worker): TemporalWorker => {
  let completion: Promise<void> | undefined
  let closing: Promise<void> | undefined
  const track = <A>(run: () => Promise<A>): Promise<A> => {
    const promise = run()
    // A second (invalid) run must not replace the first run's completion.
    completion ??= promise.then(() => undefined, () => undefined)
    return promise
  }
  const shutdown = Effect.promise(() =>
    closing ??= (async () => {
      if (completion === undefined && worker.getState() === "INITIALIZED") {
        // The SDK releases native references in run's finally block. Drive an
        // unused worker through an immediately completed runUntil to release it.
        track(() => worker.runUntil(async () => undefined))
      } else if (worker.getState() === "RUNNING") {
        worker.shutdown()
      }
      await completion
    })()
  )

  return TemporalWorker.of({
    unsafeWorker: worker,
    run: wrap("Temporal worker failed while running", () => track(() => worker.run())).pipe(
      Effect.onInterrupt(() => shutdown)
    ),
    runUntil: <A>(thunk: () => Promise<A>) =>
      wrap("Temporal worker failed while running", () => track(() => worker.runUntil(thunk))).pipe(
        Effect.onInterrupt(() => shutdown)
      ),
    shutdown
  })
}

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeConnection = (
  options: TemporalWorkerConnectionConfig = {}
): Effect.Effect<TemporalWorkerConnection, TemporalWorkerError, Scope.Scope> =>
  Effect.acquireRelease(
    wrap("Failed to connect Temporal worker runtime", () => NativeConnection.connect(options)),
    (connection) => Effect.promise(() => connection.close()).pipe(Effect.orDie)
  )

/**
 * @since 1.0.0
 * @category Layers
 */
export const connectionLayer = (
  options: TemporalWorkerConnectionConfig = {}
): Layer.Layer<TemporalWorkerConnection, TemporalWorkerError> =>
  Layer.effect(TemporalWorkerConnection)(makeConnection(options))

/**
 * @since 1.0.0
 * @category Constructors
 */
export const make = (
  options: Omit<WorkerOptions, "connection">
): Effect.Effect<TemporalWorker, TemporalWorkerError, TemporalWorkerConnection | Scope.Scope> =>
  Effect.gen(function*() {
    const connection = yield* TemporalWorkerConnection
    return yield* Effect.acquireRelease(
      wrap("Failed to create Temporal worker", () => Worker.create({ ...options, connection })).pipe(
        Effect.map(ownWorker)
      ),
      (worker) => worker.shutdown
    )
  })

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeWithDataConverter = (
  options: Omit<WorkerOptions, "connection" | "dataConverter">
): Effect.Effect<
  TemporalWorker,
  TemporalWorkerError,
  TemporalWorkerConnection | TemporalDataConverter.TemporalDataConverter | Scope.Scope
> =>
  Effect.gen(function*() {
    const dataConverter = yield* TemporalDataConverter.TemporalDataConverter
    return yield* make({ ...options, dataConverter })
  })

/**
 * @since 1.0.0
 * @category Layers
 */
export const layer = (
  options: Omit<WorkerOptions, "connection">
): Layer.Layer<TemporalWorker, TemporalWorkerError, TemporalWorkerConnection> =>
  Layer.effect(TemporalWorker)(make(options))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWithDataConverter = (
  options: Omit<WorkerOptions, "connection" | "dataConverter">
): Layer.Layer<
  TemporalWorker,
  TemporalWorkerError,
  TemporalWorkerConnection | TemporalDataConverter.TemporalDataConverter
> => Layer.effect(TemporalWorker)(makeWithDataConverter(options))
