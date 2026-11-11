/**
 * @since 1.0.0
 */
import * as Data from "effect/Data"

/**
 * @since 1.0.0
 * @category Errors
 */
export {
  TemporalClientError,
  TemporalClientError as TemporalRequestError,
  TemporalConnectionError,
  TemporalValidationError
} from "@effect-temporal/client/TemporalError"

/**
 * @since 1.0.0
 * @category Errors
 */
export class TemporalWorkflowEngineError extends Data.TaggedError("TemporalWorkflowEngineError")<{
  readonly message: string
}> {}

/**
 * @since 1.0.0
 * @category Errors
 */
export class TemporalWorkerError extends Data.TaggedError("TemporalWorkerError")<{
  readonly message: string
  readonly cause: unknown
}> {}
