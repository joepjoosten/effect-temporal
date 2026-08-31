/**
 * @since 1.0.0
 */

/**
 * @since 1.0.0
 * @category Models
 */
export type TemporalWorkflowStatus = "running" | "suspended" | "completed"

/**
 * Workflow state served by the reserved state query. `result` holds the
 * schema-encoded JSON form of the workflow's `Workflow.Result` (a completed
 * exit, or a suspension including its cause), produced by
 * `TemporalWorkflowWire.wireCodecsFor`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalWorkflowState {
  readonly executionId: string
  readonly status: TemporalWorkflowStatus
  readonly result?: unknown
}

/**
 * Deferred lookup response. `exit` is the JSON wire form produced by
 * `TemporalWorkflowWire.encodeDeferredExit`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalDeferredResult {
  readonly found: boolean
  readonly exit?: unknown
}

/**
 * Deferred completion signal. `exit` is the JSON wire form produced by
 * `TemporalWorkflowWire.encodeDeferredExit`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface CompleteDeferredSignal {
  readonly name: string
  readonly exit: unknown
}

/**
 * @since 1.0.0
 * @category Models
 */
export interface ScheduleClockSignal {
  readonly name: string
  readonly durationMs: number
}

/**
 * @since 1.0.0
 * @category Constants
 */
export const workflowStateQueryName = "__effect_workflow_state"

/**
 * @since 1.0.0
 * @category Constants
 */
export const deferredResultQueryName = "__effect_workflow_deferred_result"

/**
 * @since 1.0.0
 * @category Constants
 */
export const interruptSignalName = "__effect_workflow_interrupt"

/**
 * @since 1.0.0
 * @category Constants
 */
export const resumeSignalName = "__effect_workflow_resume"

/**
 * @since 1.0.0
 * @category Constants
 */
export const completeDeferredSignalName = "__effect_workflow_complete_deferred"

/**
 * @since 1.0.0
 * @category Constants
 */
export const scheduleClockSignalName = "__effect_workflow_schedule_clock"

/**
 * @since 1.0.0
 * @category Constants
 */
export const stateCellQueryName = "__effect_workflow_state_cell"

/**
 * @since 1.0.0
 * @category Constants
 */
export const mailboxSignalName = "__effect_workflow_mailbox"

/**
 * Mailbox message signal. `payload` is the schema-encoded JSON form of the
 * message.
 *
 * @since 1.0.0
 * @category Models
 */
export interface MailboxSignal {
  readonly name: string
  readonly payload: unknown
}

/**
 * State-cell lookup response. `value` is the schema-encoded JSON form of the
 * cell's current value.
 *
 * @since 1.0.0
 * @category Models
 */
export interface TemporalStateCellResult {
  readonly found: boolean
  readonly value?: unknown
}

/**
 * @since 1.0.0
 * @category Helpers
 */
export const workflowIdFor = (
  workflowName: string,
  executionId: string,
  workflowIdPrefix?: string | undefined
): string => `${workflowIdPrefix ?? workflowName}/${executionId}`
