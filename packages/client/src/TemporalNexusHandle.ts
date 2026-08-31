/**
 * Effect wrappers for Temporal Nexus operation handles, typed against the
 * real `@temporalio/client` Nexus surface.
 *
 * @since 1.0.0
 */
import type {
  DescribeNexusOperationOptions,
  NexusOperationExecutionDescription,
  NexusOperationHandle
} from "@temporalio/client"
import type * as Effect from "effect/Effect"
import { type TemporalClientError, tryClientPromise } from "./TemporalError.js"

/**
 * Re-exported from `@temporalio/client`.
 *
 * @since 1.0.0
 * @category Re-exports
 */
export type { DescribeNexusOperationOptions, NexusOperationExecutionDescription } from "@temporalio/client"

/**
 * The raw SDK handle for a standalone Nexus operation.
 *
 * @since 1.0.0
 * @category Models
 */
export type UnsafeNexusOperationHandle<O = unknown> = NexusOperationHandle<O>

/**
 * @since 1.0.0
 * @category Models
 */
export interface TemporalNexusOperationHandle<O = unknown> {
  readonly unsafeHandle: UnsafeNexusOperationHandle<O>
  readonly operationId: string
  readonly runId?: string | undefined
  readonly result: Effect.Effect<O, TemporalClientError>
  readonly describe: (
    options?: DescribeNexusOperationOptions | undefined
  ) => Effect.Effect<NexusOperationExecutionDescription, TemporalClientError>
  readonly cancel: (reason?: string | undefined) => Effect.Effect<void, TemporalClientError>
  readonly terminate: (reason?: string | undefined) => Effect.Effect<void, TemporalClientError>
}

/**
 * @since 1.0.0
 * @category Constructors
 */
export const fromUnsafe = <O>(
  handle: UnsafeNexusOperationHandle<O>
): TemporalNexusOperationHandle<O> => ({
  unsafeHandle: handle,
  operationId: handle.operationId,
  runId: handle.runId,
  result: tryClientPromise("NexusOperationHandle.result", () => handle.result()),
  describe: (options) => tryClientPromise("NexusOperationHandle.describe", () => handle.describe(options)),
  cancel: (reason) => tryClientPromise("NexusOperationHandle.cancel", () => handle.cancel(reason)),
  terminate: (reason) => tryClientPromise("NexusOperationHandle.terminate", () => handle.terminate(reason))
})
