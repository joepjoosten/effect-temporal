/**
 * A recording stub (test double) for the Temporal workflow client, for fast
 * unit tests of client-side code with zero servers.
 *
 * Every interaction is recorded for assertions. Any member of the client or
 * handle surface that the stub does not implement — and that the test did not
 * override — throws a descriptive error naming the member, instead of
 * returning `undefined`.
 *
 * @since 1.0.0
 */
import { TemporalClientError } from "@effect-temporal/client/TemporalError"
import * as TemporalClient from "@effect-temporal/client/TemporalWorkflowClient"
import type * as TemporalWorkflowHandle from "@effect-temporal/client/TemporalWorkflowHandle"
import { WorkflowExecutionAlreadyStartedError } from "@temporalio/client"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"

/**
 * @since 1.0.0
 * @category Models
 */
export interface RecordedStart {
  readonly workflow: string
  readonly workflowId: string
  readonly options: unknown
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface RecordedSignal {
  readonly workflowId: string
  readonly signalName: string
  readonly args: ReadonlyArray<unknown>
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface RecordedQuery {
  readonly workflowId: string
  readonly queryName: string
  readonly args: ReadonlyArray<unknown>
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface RecordedTermination {
  readonly workflowId: string
  readonly reason?: string | undefined
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface StubTemporalClientOptions {
  /**
   * Workflow ids for which `start` fails with
   * `WorkflowExecutionAlreadyStartedError`, simulating an execution that is
   * already running (or, under `REJECT_DUPLICATE`, already completed).
   */
  readonly alreadyStartedWorkflowIds?: ReadonlyArray<string> | undefined
  /**
   * Result served for `result` / `execute` / handle `result` by workflow id.
   */
  readonly resultFor?: ((workflowId: string) => unknown) | undefined
  /**
   * Response served for queries. Defaults to throwing a descriptive error.
   */
  readonly queryFor?: ((workflowId: string, queryName: string, args: ReadonlyArray<unknown>) => unknown) | undefined
  /**
   * Direct overrides for any member of the client surface; takes precedence
   * over the built-in stub behavior.
   */
  readonly overrides?: Partial<TemporalClient.TemporalWorkflowClient> | undefined
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface StubTemporalClient {
  readonly client: TemporalClient.TemporalWorkflowClient
  readonly layer: Layer.Layer<TemporalClient.TemporalWorkflowClient>
  readonly recorded: {
    readonly starts: Array<RecordedStart>
    readonly signals: Array<RecordedSignal>
    readonly queries: Array<RecordedQuery>
    readonly terminations: Array<RecordedTermination>
    readonly cancellations: Array<string>
  }
}

const unconfigured = (surface: string, member: PropertyKey): never => {
  throw new Error(
    `Stub Temporal client: "${surface}.${String(member)}" is not configured. `
      + `Pass an implementation via StubTemporalClientOptions to use it in this test.`
  )
}

const stubProxy = <T extends object>(surface: string, behavior: object): T =>
  new Proxy(behavior, {
    get(target, member) {
      if (member in target) {
        return (target as Record<PropertyKey, unknown>)[member]
      }
      return unconfigured(surface, member)
    }
  }) as T

/**
 * Creates a recording stub implementing the Temporal workflow client surface.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const makeStubTemporalClient = (
  options: StubTemporalClientOptions = {}
): StubTemporalClient => {
  const recorded: StubTemporalClient["recorded"] = {
    starts: [],
    signals: [],
    queries: [],
    terminations: [],
    cancellations: []
  }
  const alreadyStarted = new Set(options.alreadyStartedWorkflowIds ?? [])

  const resultEffect = (workflowId: string): Effect.Effect<any, TemporalClientError> =>
    options.resultFor === undefined
      ? Effect.sync(() => unconfigured("StubTemporalClient", `resultFor(${workflowId})`))
      : Effect.sync(() => options.resultFor!(workflowId))

  const makeHandle = (workflowId: string): TemporalWorkflowHandle.TemporalWorkflowHandle<any> =>
    stubProxy("TemporalWorkflowHandle", {
      workflowId,
      result: resultEffect(workflowId),
      signal: (signalName: string, ...args: Array<unknown>) =>
        Effect.sync(() => {
          recorded.signals.push({ args, signalName, workflowId })
        }),
      query: (queryName: string, ...args: Array<unknown>) =>
        Effect.sync(() => {
          recorded.queries.push({ args, queryName, workflowId })
          return options.queryFor === undefined
            ? unconfigured("StubTemporalClient", `queryFor(${workflowId}, ${queryName})`)
            : options.queryFor(workflowId, queryName, args)
        }),
      terminate: (reason?: string) =>
        Effect.sync(() => {
          recorded.terminations.push({ reason, workflowId })
        }),
      cancel: Effect.sync(() => {
        recorded.cancellations.push(workflowId)
      })
    })

  const behavior: Partial<TemporalClient.TemporalWorkflowClient> = {
    start: ((workflow: any, startOptions: any) =>
      Effect.suspend(() => {
        const workflowId = startOptions.workflowId as string
        recorded.starts.push({
          options: startOptions,
          workflow: typeof workflow === "string" ? workflow : workflow.name,
          workflowId
        })
        if (alreadyStarted.has(workflowId)) {
          return Effect.fail(
            new TemporalClientError({
              cause: new WorkflowExecutionAlreadyStartedError(
                "Workflow execution already started",
                workflowId,
                typeof workflow === "string" ? workflow : workflow.name
              ),
              message: "Workflow execution already started",
              operation: "WorkflowClient.start"
            })
          )
        }
        return Effect.succeed(makeHandle(workflowId))
      })) as TemporalClient.TemporalWorkflowClient["start"],
    execute: ((workflow: any, startOptions: any) =>
      Effect.suspend(() => {
        const workflowId = startOptions.workflowId as string
        recorded.starts.push({
          options: startOptions,
          workflow: typeof workflow === "string" ? workflow : workflow.name,
          workflowId
        })
        return resultEffect(workflowId)
      })) as TemporalClient.TemporalWorkflowClient["execute"],
    result: ((workflowId: string) => resultEffect(workflowId)) as TemporalClient.TemporalWorkflowClient["result"],
    getHandle: ((workflowId: string) => makeHandle(workflowId)) as TemporalClient.TemporalWorkflowClient["getHandle"],
    ...options.overrides
  }

  const client = stubProxy<TemporalClient.TemporalWorkflowClient>("StubTemporalClient", behavior)

  return {
    client,
    layer: Layer.succeed(TemporalClient.TemporalWorkflowClient)(client),
    recorded
  }
}
