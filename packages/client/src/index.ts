/**
 * Effect service for Temporal standalone activities and async completion.
 *
 * @since 1.0.0
 */
export * as TemporalActivityClient from "./TemporalActivityClient.js"

/**
 * Effect wrappers for Temporal activity handles.
 *
 * @since 1.0.0
 */
export * as TemporalActivityHandle from "./TemporalActivityHandle.js"

/**
 * Effect service for the high-level Temporal SDK client.
 *
 * @since 1.0.0
 */
export * as TemporalClient from "./TemporalClient.js"

/**
 * Effect service for Temporal client connections.
 *
 * @since 1.0.0
 */
export * as TemporalConnection from "./TemporalConnection.js"

/**
 * Effect services and helpers for Temporal data converters and payload codecs.
 *
 * @since 1.0.0
 */
export * as TemporalDataConverter from "./TemporalDataConverter.js"

/**
 * @since 1.0.0
 */
export * as TemporalError from "./TemporalError.js"

/**
 * Effect service for Temporal Nexus standalone operations.
 *
 * The local Temporal SDK source tree has Nexus support that is not present in
 * every published `@temporalio/client` build. This module uses structural
 * types and dynamic construction so the package still typechecks against the
 * published SDK while supporting local SDK builds that expose Nexus.
 *
 * @since 1.0.0
 */
export * as TemporalNexusClient from "./TemporalNexusClient.js"

/**
 * Effect wrappers for Temporal Nexus operation handles.
 *
 * The published Temporal SDK may not expose Nexus yet, while local SDK builds
 * can. These types are structural so the wrapper can support both.
 *
 * @since 1.0.0
 */
export * as TemporalNexusHandle from "./TemporalNexusHandle.js"

/**
 * Effect service for Temporal schedules.
 *
 * @since 1.0.0
 */
export * as TemporalScheduleClient from "./TemporalScheduleClient.js"

/**
 * Effect wrappers for Temporal schedule handles.
 *
 * @since 1.0.0
 */
export * as TemporalScheduleHandle from "./TemporalScheduleHandle.js"

/**
 * Effect Schema helpers for validating Temporal client boundaries.
 *
 * @since 1.0.0
 */
export * as TemporalSchema from "./TemporalSchema.js"

/**
 * Effect service for Temporal task queue operations.
 *
 * @since 1.0.0
 */
export * as TemporalTaskQueueClient from "./TemporalTaskQueueClient.js"

/**
 * Effect service for Temporal workflow operations.
 *
 * @since 1.0.0
 */
export * as TemporalWorkflowClient from "./TemporalWorkflowClient.js"

/**
 * Effect wrappers for Temporal workflow handles.
 *
 * @since 1.0.0
 */
export * as TemporalWorkflowHandle from "./TemporalWorkflowHandle.js"
