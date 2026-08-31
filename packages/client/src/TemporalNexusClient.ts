/**
 * Effect service for Temporal Nexus standalone operations, typed against the
 * real `@temporalio/client` Nexus surface.
 *
 * @since 1.0.0
 */
import type {
  GetNexusOperationHandleOptions,
  ListNexusOperationsOptions,
  NexusClientOptions,
  NexusOperationExecution,
  NexusOperationExecutionCount,
  NexusServiceClient,
  StartNexusOperationOptions
} from "@temporalio/client"
import { NexusClient } from "@temporalio/client"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Stream from "effect/Stream"
import type * as NexusRpc from "nexus-rpc"
import * as TemporalConnection from "./TemporalConnection.js"
import * as TemporalDataConverter from "./TemporalDataConverter.js"
import { streamClientIterable, type TemporalClientError, tryClientPromise, tryClientSync } from "./TemporalError.js"
import * as TemporalNexusHandle from "./TemporalNexusHandle.js"

/**
 * Re-exported from `@temporalio/client`.
 *
 * @since 1.0.0
 * @category Re-exports
 */
export type {
  GetNexusOperationHandleOptions,
  ListNexusOperationsOptions,
  NexusOperationExecution,
  NexusOperationExecutionCount,
  StartNexusOperationOptions
} from "@temporalio/client"

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalNexusClientConfig = Omit<NexusClientOptions, "connection">

/**
 * The raw SDK Nexus client.
 *
 * @since 1.0.0
 * @category Models
 */
export type UnsafeNexusClient = NexusClient

/**
 * The raw SDK service client for one endpoint + service pair.
 *
 * @since 1.0.0
 * @category Models
 */
export type UnsafeNexusServiceClient<T extends NexusRpc.ServiceDefinition> = NexusServiceClient<T>

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalNexusServiceClient<T extends NexusRpc.ServiceDefinition> {
  readonly unsafeServiceClient: UnsafeNexusServiceClient<T>
  readonly endpoint: string
  readonly service: T
  readonly startOperation: <Op extends T["operations"][keyof T["operations"]]>(
    operation: Op,
    input: NexusRpc.OperationInput<Op>,
    options: StartNexusOperationOptions
  ) => Effect.Effect<
    TemporalNexusHandle.TemporalNexusOperationHandle<NexusRpc.OperationOutput<Op>>,
    TemporalClientError
  >
  readonly executeOperation: <Op extends T["operations"][keyof T["operations"]]>(
    operation: Op,
    input: NexusRpc.OperationInput<Op>,
    options: StartNexusOperationOptions
  ) => Effect.Effect<NexusRpc.OperationOutput<Op>, TemporalClientError>
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalNexusClient {
  readonly unsafeClient: UnsafeNexusClient
  readonly createServiceClient: <T extends NexusRpc.ServiceDefinition>(options: {
    readonly endpoint: string
    readonly service: T
  }) => TemporalNexusServiceClient<T>
  readonly getHandle: <O = unknown>(
    operationId: string,
    options?: GetNexusOperationHandleOptions | undefined
  ) => TemporalNexusHandle.TemporalNexusOperationHandle<O>
  readonly list: (
    options?: ListNexusOperationsOptions | undefined
  ) => Stream.Stream<NexusOperationExecution, TemporalClientError>
  readonly count: (query?: string | undefined) => Effect.Effect<NexusOperationExecutionCount, TemporalClientError>
}

/**
 * @since 1.0.0
 * @category Tags
 */
export const TemporalNexusClient = Context.Service<TemporalNexusClient>(
  "@effect-temporal/client/TemporalNexusClient"
)

const fromUnsafeServiceClient = <T extends NexusRpc.ServiceDefinition>(
  serviceClient: UnsafeNexusServiceClient<T>
): TemporalNexusServiceClient<T> => ({
  unsafeServiceClient: serviceClient,
  endpoint: serviceClient.endpoint,
  service: serviceClient.service,
  startOperation: (operation, input, options) =>
    tryClientPromise("NexusServiceClient.startOperation", () => serviceClient.startOperation(operation, input, options))
      .pipe(
        Effect.map(TemporalNexusHandle.fromUnsafe)
      ),
  executeOperation: (operation, input, options) =>
    tryClientPromise(
      "NexusServiceClient.executeOperation",
      () => serviceClient.executeOperation(operation, input, options)
    )
})

/**
 * @since 1.0.0
 * @category Constructors
 */
export const fromUnsafe = (client: UnsafeNexusClient): TemporalNexusClient =>
  TemporalNexusClient.of({
    unsafeClient: client,
    createServiceClient: (options) => fromUnsafeServiceClient(client.createServiceClient(options)),
    getHandle: <O = unknown>(operationId: string, options?: GetNexusOperationHandleOptions | undefined) =>
      TemporalNexusHandle.fromUnsafe(
        client.getHandle(operationId, options) as TemporalNexusHandle.UnsafeNexusOperationHandle<O>
      ),
    list: (options) => streamClientIterable("NexusClient.list", client.list(options)),
    count: (query) => tryClientPromise("NexusClient.count", () => client.count(query))
  })

/**
 * @since 1.0.0
 * @category Constructors
 */
export const make = (
  options: TemporalNexusClientConfig = {}
): Effect.Effect<TemporalNexusClient, TemporalClientError, TemporalConnection.TemporalConnection> =>
  Effect.gen(function*() {
    const connection = yield* TemporalConnection.TemporalConnection
    return fromUnsafe(
      yield* tryClientSync(
        "NexusClient.constructor",
        () => new NexusClient({ ...options, connection: connection.unsafeConnection })
      )
    )
  })

/**
 * @since 1.0.0
 * @category Constructors
 */
export const makeWithDataConverter = (
  options: Omit<TemporalNexusClientConfig, "dataConverter"> = {}
): Effect.Effect<
  TemporalNexusClient,
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
  options: TemporalNexusClientConfig = {}
): Layer.Layer<TemporalNexusClient, TemporalClientError, TemporalConnection.TemporalConnection> =>
  Layer.effect(TemporalNexusClient)(make(options))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerWithDataConverter = (
  options: Omit<TemporalNexusClientConfig, "dataConverter"> = {}
): Layer.Layer<
  TemporalNexusClient,
  TemporalClientError,
  TemporalConnection.TemporalConnection | TemporalDataConverter.TemporalDataConverter
> => Layer.effect(TemporalNexusClient)(makeWithDataConverter(options))

/**
 * @since 1.0.0
 * @category Layers
 */
export const layerFromUnsafe = (client: UnsafeNexusClient): Layer.Layer<TemporalNexusClient> =>
  Layer.succeed(TemporalNexusClient)(fromUnsafe(client))
