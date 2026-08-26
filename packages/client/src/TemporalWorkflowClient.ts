/**
 * Effect service for Temporal workflow operations.
 *
 * @since 1.0.0
 */
import {
  type CountWorkflowExecution,
  type GetWorkflowHandleOptions,
  type IntoHistoriesOptions,
  type ListOptions,
  type UpdateDefinition,
  WithStartWorkflowOperation,
  type WithWorkflowArgs,
  type Workflow,
  WorkflowClient,
  type WorkflowClientOptions,
  type WorkflowExecutionInfo,
  type WorkflowIdConflictPolicy,
  type WorkflowResultOptions,
  type WorkflowResultType,
  type WorkflowSignalWithStartOptions,
  type WorkflowStartOptions,
  type WorkflowUpdateOptions
} from "@temporalio/client"
import { executionInfoFromRaw } from "@temporalio/client/lib/helpers.js"
import {
  decodeArrayFromPayloads,
  decodeOptionalFailureToOptionalError
} from "@temporalio/common/lib/internal-non-workflow/codec-helpers.js"
import type { temporal } from "@temporalio/proto"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import * as Stream from "effect/Stream"
import * as TemporalConnection from "./TemporalConnection.js"
import * as TemporalDataConverter from "./TemporalDataConverter.js"
import {
  streamClientIterable,
  type TemporalClientError,
  type TemporalValidationError,
  tryClientPromise,
  tryClientSync
} from "./TemporalError.js"
import * as TemporalSchema from "./TemporalSchema.js"
import * as TemporalWorkflowHandle from "./TemporalWorkflowHandle.js"

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalWorkflowClientConfig = Omit<WorkflowClientOptions, "connection">

/**
 * @since 1.0.0
 * @category Models
 */
export type WorkflowHistoryEntry = ReturnType<
  ReturnType<WorkflowClient["list"]>["intoHistories"]
> extends AsyncIterable<infer A> ? A : never

/**
 * @since 1.0.0
 * @category Models
 */
export type WorkflowStartOptionsWithArgs<T extends Workflow, Args extends TemporalSchema.Args> =
  & Omit<
    WorkflowStartOptions<T>,
    "args"
  >
  & {
    readonly args?: TemporalSchema.Encoded<Args> | undefined
  }

/**
 * Re-exported from `@temporalio/client` so consumers do not need a direct
 * dependency on the Temporal SDK for common workflow types and errors.
 *
 * @since 1.0.0
 * @category Re-exports
 */
export { WorkflowFailedError } from "@temporalio/client"

/**
 * Re-exported from `@temporalio/client`.
 *
 * @since 1.0.0
 * @category Re-exports
 */
export type { ListOptions, WorkflowExecutionInfo } from "@temporalio/client"

/**
 * A raw Temporal history event as returned by the workflow service.
 *
 * @since 1.0.0
 * @category Models
 */
export type HistoryEvent = temporal.api.history.v1.IHistoryEvent

/**
 * A raw Temporal payload as it appears in history events.
 *
 * @since 1.0.0
 * @category Models
 */
export type RawPayload = temporal.api.common.v1.IPayload

/**
 * A raw Temporal failure as it appears in history events.
 *
 * @since 1.0.0
 * @category Models
 */
export type RawFailure = temporal.api.failure.v1.IFailure

/**
 * Progress information reported after each page fetched by `listPaged`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface ListPagedProgress {
  readonly page: number
  readonly requestedPageSize: number
  readonly receivedItems: number
  readonly emittedItems: number
  readonly totalEmittedItems: number
  readonly hasNextPage: boolean
}

/**
 * Options for `listPaged`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface ListPagedOptions {
  readonly query?: string | undefined
  readonly pageSize?: number | undefined
  readonly limit?: number | undefined
  readonly onPage?: ((progress: ListPagedProgress) => Effect.Effect<void>) | undefined
}

/**
 * Options for `fetchHistoryEvents`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface FetchHistoryEventsOptions {
  readonly pageSize?: number | undefined
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalWorkflowClient {
  readonly unsafeClient: WorkflowClient
  readonly start: <T extends Workflow>(
    workflow: string | T,
    options: WorkflowStartOptions<T>
  ) => Effect.Effect<TemporalWorkflowHandle.TemporalWorkflowHandle<T>, TemporalClientError>
  readonly startWithSchema: <T extends Workflow, Args extends TemporalSchema.Args>(
    workflow: string | T,
    options: WorkflowStartOptionsWithArgs<T, Args>,
    schema: TemporalSchema.ArgsSchema<Args>
  ) => Effect.Effect<
    TemporalWorkflowHandle.TemporalWorkflowHandle<T>,
    TemporalClientError | TemporalValidationError,
    TemporalSchema.DecodingServices<Args>
  >
  readonly signalWithStart: <T extends Workflow, SignalArgs extends any[] = []>(
    workflow: string | T,
    options: WithWorkflowArgs<T, WorkflowSignalWithStartOptions<SignalArgs>>
  ) => Effect.Effect<TemporalWorkflowHandle.TemporalWorkflowHandle<T>, TemporalClientError>
  readonly execute: <T extends Workflow>(
    workflow: string | T,
    options: WorkflowStartOptions<T>
  ) => Effect.Effect<WorkflowResultType<T>, TemporalClientError>
  readonly executeWithSchema: <
    T extends Workflow,
    Args extends TemporalSchema.Args,
    Result extends TemporalSchema.Any
  >(
    workflow: string | T,
    options: WorkflowStartOptionsWithArgs<T, Args>,
    schema: TemporalSchema.ArgsAndResultSchema<Args, Result>
  ) => Effect.Effect<
    TemporalSchema.Type<Result>,
    TemporalClientError | TemporalValidationError,
    TemporalSchema.DecodingServices<Args> | TemporalSchema.DecodingServices<Result>
  >
  readonly result: <T extends Workflow>(
    workflowId: string,
    runId?: string | undefined,
    options?: WorkflowResultOptions | undefined
  ) => Effect.Effect<WorkflowResultType<T>, TemporalClientError>
  readonly resultAs: <Result extends TemporalSchema.Any>(
    workflowId: string,
    schema: TemporalSchema.ResultSchema<Result>,
    runId?: string | undefined,
    options?: WorkflowResultOptions | undefined
  ) => Effect.Effect<
    TemporalSchema.Type<Result>,
    TemporalClientError | TemporalValidationError,
    TemporalSchema.DecodingServices<Result>
  >
  readonly getHandle: <T extends Workflow>(
    workflowId: string,
    runId?: string | undefined,
    options?: GetWorkflowHandleOptions | undefined
  ) => TemporalWorkflowHandle.TemporalWorkflowHandle<T>
  readonly list: (
    options?: ListOptions | undefined
  ) => Stream.Stream<WorkflowExecutionInfo, TemporalClientError>
  readonly listPaged: (
    options?: ListPagedOptions | undefined
  ) => Stream.Stream<WorkflowExecutionInfo, TemporalClientError>
  readonly fetchHistoryEvents: (
    workflowId: string,
    runId?: string | undefined,
    options?: FetchHistoryEventsOptions | undefined
  ) => Effect.Effect<ReadonlyArray<HistoryEvent>, TemporalClientError>
  readonly startInput: (
    workflowId: string,
    runId?: string | undefined
  ) => Effect.Effect<ReadonlyArray<unknown>, TemporalClientError>
  readonly decodePayloads: (
    payloads: ReadonlyArray<RawPayload> | null | undefined
  ) => Effect.Effect<ReadonlyArray<unknown>, TemporalClientError>
  readonly decodeFailure: (
    failure: RawFailure | null | undefined
  ) => Effect.Effect<Error | undefined, TemporalClientError>
  readonly listHistories: (
    options?: ListOptions | undefined,
    intoHistoriesOptions?: IntoHistoriesOptions | undefined
  ) => Stream.Stream<WorkflowHistoryEntry, TemporalClientError>
  readonly count: (query?: string | undefined) => Effect.Effect<CountWorkflowExecution, TemporalClientError>
  readonly startUpdateWithStart: <T extends Workflow, Ret, Args extends any[]>(
    definition: UpdateDefinition<Ret, Args> | string,
    options: WorkflowUpdateOptions & {
      readonly args?: Args | undefined
      readonly waitForStage: "ACCEPTED"
      readonly startWorkflowOperation: WithStartWorkflowOperation<T>
    }
  ) => Effect.Effect<TemporalWorkflowHandle.TemporalWorkflowUpdateHandle<Ret>, TemporalClientError>
  readonly executeUpdateWithStart: <T extends Workflow, Ret, Args extends any[]>(
    definition: UpdateDefinition<Ret, Args> | string,
    options: WorkflowUpdateOptions & {
      readonly args?: Args | undefined
      readonly startWorkflowOperation: WithStartWorkflowOperation<T>
    }
  ) => Effect.Effect<Ret, TemporalClientError>
  readonly withStartWorkflowOperation: <T extends Workflow>(
    workflow: string | T,
    options: WorkflowStartOptions<T> & { readonly workflowIdConflictPolicy: WorkflowIdConflictPolicy }
  ) => WithStartWorkflowOperation<T>
}

/**
 * @since 1.0.0
 * @category Tags
 */
export const TemporalWorkflowClient = Context.Service<TemporalWorkflowClient>(
  "@effect-temporal/client/TemporalWorkflowClient"
)

const DEFAULT_LIST_PAGE_SIZE = 1000

interface ListPagedState {
  readonly nextPageToken: Uint8Array
  readonly remaining: number | undefined
  readonly page: number
  readonly emitted: number
}

const hasNextPageToken = (nextPageToken: Uint8Array | null | undefined): nextPageToken is Uint8Array =>
  nextPageToken !== undefined && nextPageToken !== null && nextPageToken.length > 0

const listPaged = (
  client: WorkflowClient,
  options: ListPagedOptions = {}
): Stream.Stream<WorkflowExecutionInfo, TemporalClientError> => {
  const pageSize = options.pageSize ?? DEFAULT_LIST_PAGE_SIZE
  const initialState: ListPagedState = {
    nextPageToken: new Uint8Array(),
    remaining: options.limit !== undefined && options.limit > 0 ? options.limit : undefined,
    page: 1,
    emitted: 0
  }

  return Stream.paginate(initialState, (state) => {
    if (state.remaining !== undefined && state.remaining <= 0) {
      return Effect.succeed([[], Option.none<ListPagedState>()] as const)
    }

    const requestedPageSize = state.remaining === undefined ? pageSize : Math.min(pageSize, state.remaining)

    return tryClientPromise("WorkflowClient.listPaged", () =>
      client.workflowService.listWorkflowExecutions({
        namespace: client.options.namespace,
        query: options.query ?? null,
        nextPageToken: state.nextPageToken,
        pageSize: requestedPageSize
      })).pipe(
        Effect.flatMap((response) => {
          const executions = response.executions ?? []
          const emittedItems = state.remaining === undefined ? executions : executions.slice(0, state.remaining)
          const remaining = state.remaining === undefined ? undefined : state.remaining - emittedItems.length
          const totalEmittedItems = state.emitted + emittedItems.length
          const nextPageToken = response.nextPageToken
          const hasNextPage = hasNextPageToken(nextPageToken) && (remaining === undefined || remaining > 0)
          const nextState = hasNextPage
            ? Option.some<ListPagedState>({
              nextPageToken,
              remaining,
              page: state.page + 1,
              emitted: totalEmittedItems
            })
            : Option.none<ListPagedState>()

          return (options.onPage?.({
            page: state.page,
            requestedPageSize,
            receivedItems: executions.length,
            emittedItems: emittedItems.length,
            totalEmittedItems,
            hasNextPage
          }) ?? Effect.void).pipe(
            Effect.as([emittedItems, nextState] as const)
          )
        })
      )
  }).pipe(
    Stream.mapEffect((raw) =>
      tryClientPromise(
        "WorkflowClient.listPaged.executionInfo",
        () => executionInfoFromRaw(raw, client.options.loadedDataConverter, raw)
      )
    )
  )
}

const fetchHistoryEvents = async (
  client: WorkflowClient,
  workflowId: string,
  runId: string | undefined,
  options: FetchHistoryEventsOptions | undefined
): Promise<ReadonlyArray<HistoryEvent>> => {
  let nextPageToken: Uint8Array | undefined
  const events: Array<HistoryEvent> = []

  do {
    const response = await client.workflowService.getWorkflowExecutionHistory({
      namespace: client.options.namespace,
      execution: { workflowId, runId: runId ?? null },
      maximumPageSize: options?.pageSize ?? null,
      nextPageToken: nextPageToken ?? null
    })

    for (const event of response.history?.events ?? []) {
      events.push(event)
    }
    nextPageToken = hasNextPageToken(response.nextPageToken) ? response.nextPageToken : undefined
  } while (nextPageToken)

  return events
}

const startInput = async (
  client: WorkflowClient,
  workflowId: string,
  runId: string | undefined
): Promise<ReadonlyArray<unknown>> => {
  const response = await client.workflowService.getWorkflowExecutionHistory({
    namespace: client.options.namespace,
    execution: { workflowId, runId: runId ?? null },
    maximumPageSize: 1
  })
  const attributes = response.history?.events?.find((event) => event.workflowExecutionStartedEventAttributes)
    ?.workflowExecutionStartedEventAttributes

  if (!attributes) {
    throw new Error("WorkflowExecutionStarted event was not found")
  }

  return await decodeArrayFromPayloads(client.options.loadedDataConverter, attributes.input?.payloads)
}

/**
 * @since 1.0.0
 * @category Constructors
 */
export const fromUnsafe = (client: WorkflowClient): TemporalWorkflowClient =>
  TemporalWorkflowClient.of({
    unsafeClient: client,
    start: (workflow, options) =>
      tryClientPromise("WorkflowClient.start", () => client.start(workflow, options)).pipe(
        Effect.map(TemporalWorkflowHandle.fromUnsafe)
      ),
    startWithSchema: (workflow, options, schema) =>
      TemporalSchema.decodeInput("WorkflowClient.start.args", schema.args, options.args ?? []).pipe(
        Effect.flatMap((args) =>
          tryClientPromise("WorkflowClient.start", () =>
            client.start(workflow, { ...options, args } as WorkflowStartOptions<any>))
        ),
        Effect.map(TemporalWorkflowHandle.fromUnsafe)
      ),
    signalWithStart: (workflow, options) =>
      tryClientPromise("WorkflowClient.signalWithStart", () =>
        client.signalWithStart(workflow, options)).pipe(
          Effect.map(TemporalWorkflowHandle.fromUnsafe)
        ),
    execute: (workflow, options) => tryClientPromise("WorkflowClient.execute", () => client.execute(workflow, options)),
    executeWithSchema: (workflow, options, schema) =>
      TemporalSchema.decodeInput("WorkflowClient.execute.args", schema.args, options.args ?? []).pipe(
        Effect.flatMap((args) =>
          tryClientPromise("WorkflowClient.execute", () =>
            client.execute(workflow, { ...options, args } as WorkflowStartOptions<any>))
        ),
        Effect.flatMap((value) =>
          TemporalSchema.decodeOutput("WorkflowClient.execute.result", schema.result, value)
        )
      ),
    result: (workflowId, runId, options) =>
      tryClientPromise("WorkflowClient.result", () => client.result(workflowId, runId, options)),
    resultAs: (workflowId, schema, runId, options) =>
      tryClientPromise("WorkflowClient.result", () => client.result(workflowId, runId, options)).pipe(
        Effect.flatMap((value) => TemporalSchema.decodeOutput("WorkflowClient.result", schema.result, value))
      ),
    getHandle: (workflowId, runId, options) =>
      TemporalWorkflowHandle.fromUnsafe(client.getHandle(workflowId, runId, options)),
    list: (options) => streamClientIterable("WorkflowClient.list", client.list(options)),
    listPaged: (options) => listPaged(client, options),
    fetchHistoryEvents: (workflowId, runId, options) =>
      tryClientPromise(
        "WorkflowClient.fetchHistoryEvents",
        () => fetchHistoryEvents(client, workflowId, runId, options)
      ),
    startInput: (workflowId, runId) =>
      tryClientPromise("WorkflowClient.startInput", () => startInput(client, workflowId, runId)),
    decodePayloads: (payloads) =>
      tryClientPromise(
        "WorkflowClient.decodePayloads",
        () => decodeArrayFromPayloads(client.options.loadedDataConverter, payloads == null ? payloads : [...payloads])
      ),
    decodeFailure: (failure) =>
      tryClientPromise(
        "WorkflowClient.decodeFailure",
        () => decodeOptionalFailureToOptionalError(client.options.loadedDataConverter, failure)
      ),
    listHistories: (options, intoHistoriesOptions) =>
      streamClientIterable(
        "WorkflowClient.list.intoHistories",
        client.list(options).intoHistories(intoHistoriesOptions)
      ),
    count: (query) => tryClientPromise("WorkflowClient.count", () => client.count(query)),
    startUpdateWithStart: (definition, options) =>
      tryClientPromise(
        "WorkflowClient.startUpdateWithStart",
        () => client.startUpdateWithStart(definition, options as any)
      ).pipe(
        Effect.map(TemporalWorkflowHandle.fromUnsafeUpdateHandle)
      ),
    executeUpdateWithStart: (definition, options) =>
      tryClientPromise(
        "WorkflowClient.executeUpdateWithStart",
        () => client.executeUpdateWithStart(definition, options as any)
      ),
    withStartWorkflowOperation: (workflow, options) => new WithStartWorkflowOperation(workflow, options)
  })

/**
 * @since 1.0.0
 * @category Constructors
 */
export const make = (
  options: TemporalWorkflowClientConfig = {}
): Effect.Effect<TemporalWorkflowClient, TemporalClientError, TemporalConnection.TemporalConnection> =>
  Effect.gen(function*() {
    const connection = yield* TemporalConnection.TemporalConnection
    return yield* tryClientSync(
      "WorkflowClient.constructor",
      () => fromUnsafe(new WorkflowClient({ ...options, connection: connection.unsafeConnection }))
    )
  })

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeWithDataConverter = (
  options: Omit<TemporalWorkflowClientConfig, "dataConverter"> = {}
): Effect.Effect<
  TemporalWorkflowClient,
  TemporalClientError,
  TemporalConnection.TemporalConnection | TemporalDataConverter.TemporalDataConverter
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
  options: TemporalWorkflowClientConfig = {}
): Layer.Layer<TemporalWorkflowClient, TemporalClientError, TemporalConnection.TemporalConnection> =>
  Layer.effect(TemporalWorkflowClient)(make(options))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWithDataConverter = (
  options: Omit<TemporalWorkflowClientConfig, "dataConverter"> = {}
): Layer.Layer<
  TemporalWorkflowClient,
  TemporalClientError,
  TemporalConnection.TemporalConnection | TemporalDataConverter.TemporalDataConverter
> => Layer.effect(TemporalWorkflowClient)(makeWithDataConverter(options))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerFromUnsafe = (client: WorkflowClient): Layer.Layer<TemporalWorkflowClient> =>
  Layer.succeed(TemporalWorkflowClient, fromUnsafe(client))
