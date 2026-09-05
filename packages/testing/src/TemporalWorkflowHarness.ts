/**
 * A one-call, scope-bound test harness for integration tests of Effect
 * workflows: boots a Temporal test environment, runs a worker for the given
 * workflow bundle and activities for the lifetime of the scope, and hands the
 * test a ready client, engine, and a `provide` helper that wires both into
 * any Effect.
 *
 * @since 1.0.0
 */
import type * as TemporalClientError from "@effect-temporal/client/TemporalError"
import * as TemporalClient from "@effect-temporal/client/TemporalWorkflowClient"
import type * as TemporalError from "@effect-temporal/workflow/TemporalError"
import * as TemporalWorkflowEngine from "@effect-temporal/workflow/TemporalWorkflowEngine"
import * as Effect from "effect/Effect"
import type * as Scope from "effect/Scope"
import * as WorkflowEngine from "effect/unstable/workflow/WorkflowEngine"
import * as TemporalTesting from "./TemporalTesting.js"

/**
 * @since 1.0.0
 * @category Models
 */
export interface WorkflowTestHarnessOptions {
  readonly workflowsPath: string
  readonly activities?: object | undefined
  /**
   * `timeSkipping` (default) boots the time-skipping test server; `local`
   * boots a full local dev server for features the time-skipping server
   * lacks.
   */
  readonly mode?: "timeSkipping" | "local" | undefined
  readonly taskQueuePrefix?: string | undefined
  readonly workflowIdPrefix?: string | undefined
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface WorkflowTestHarness {
  readonly taskQueue: string
  readonly environment: TemporalTesting.TemporalTestEnvironment
  readonly client: TemporalClient.TemporalWorkflowClient
  readonly engine: WorkflowEngine.WorkflowEngine["Service"]
  /**
   * Provides the workflow engine, the workflow client, and the test
   * environment to the given effect.
   */
  readonly provide: <A, E, R>(
    effect: Effect.Effect<A, E, R>
  ) => Effect.Effect<
    A,
    E,
    Exclude<
      R,
      WorkflowEngine.WorkflowEngine | TemporalClient.TemporalWorkflowClient | TemporalTesting.TemporalTestEnvironment
    >
  >
}

/**
 * Boots the test environment and worker and returns a ready harness. The
 * worker runs until the surrounding scope closes; everything is torn down
 * with the scope.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const makeWorkflowTestHarness = (
  options: WorkflowTestHarnessOptions
): Effect.Effect<
  WorkflowTestHarness,
  | TemporalTesting.TemporalTestEnvironmentError
  | TemporalError.TemporalWorkerError
  | TemporalClientError.TemporalConnectionError
  | TemporalClientError.TemporalClientError,
  Scope.Scope
> =>
  Effect.gen(function*() {
    const environment = yield* (
      options.mode === "local" ? TemporalTesting.makeLocal() : TemporalTesting.makeTimeSkipping()
    )
    const withEnvironment = <A, E, R>(
      effect: Effect.Effect<A, E, R>
    ): Effect.Effect<A, E, Exclude<R, TemporalTesting.TemporalTestEnvironment>> =>
      Effect.provideService(effect, TemporalTesting.TemporalTestEnvironment, environment) as Effect.Effect<
        A,
        E,
        Exclude<R, TemporalTesting.TemporalTestEnvironment>
      >

    const taskQueue = TemporalTesting.makeTaskQueue(options.taskQueuePrefix ?? "effect-temporal-harness")
    const worker = yield* withEnvironment(TemporalTesting.makeWorker({
      taskQueue,
      workflowsPath: options.workflowsPath,
      ...(options.activities === undefined ? {} : { activities: options.activities })
    }))
    // The worker service awaits native shutdown when this scoped fiber is interrupted.
    yield* Effect.forkScoped(worker.run)

    const client = yield* withEnvironment(TemporalTesting.makeClient())
    const engine = yield* TemporalWorkflowEngine.make({
      taskQueue,
      workflowIdPrefix: options.workflowIdPrefix
    }).pipe(Effect.provideService(TemporalClient.TemporalWorkflowClient, client))

    const provide = (<A, E, R>(effect: Effect.Effect<A, E, R>) =>
      effect.pipe(
        Effect.provideService(WorkflowEngine.WorkflowEngine, engine),
        Effect.provideService(TemporalClient.TemporalWorkflowClient, client),
        Effect.provideService(TemporalTesting.TemporalTestEnvironment, environment)
      )) as WorkflowTestHarness["provide"]

    return { client, engine, environment, provide, taskQueue }
  })
