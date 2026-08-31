/**
 * Client-side interactions with running (or closed) Effect workflows beyond
 * the `WorkflowEngine` contract: reading published state cells, and — as more
 * primitives land — offering mailbox messages and executing updates.
 *
 * This module uses the Temporal client and must not be imported from a
 * workflow bundle.
 *
 * @since 1.0.0
 */
import * as Effect from "effect/Effect"
import type * as Exit from "effect/Exit"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import type * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalClient from "./TemporalClient.js"
import type { TemporalDurableMailbox } from "./TemporalDurableMailbox.js"
import type { TemporalDurableUpdate } from "./TemporalDurableUpdate.js"
import type { TemporalClientError } from "./TemporalError.js"
import type { TemporalStateCell } from "./TemporalStateCell.js"
import {
  type MailboxSignal,
  mailboxSignalName,
  stateCellQueryName,
  type TemporalStateCellResult,
  type TemporalUpdateResult,
  updateResultQueryName,
  type UpdateSignal,
  updateSignalName,
  workflowIdFor
} from "./TemporalWorkflowProtocol.js"

/**
 * Options addressing one workflow execution, mirroring the
 * `TemporalWorkflowEngine` configuration used to start it.
 *
 * @since 1.0.0
 * @category Models
 */
export interface WorkflowTarget {
  readonly workflow: Workflow.Any
  readonly executionId: string
  readonly workflowIdPrefix?: string | undefined
}

/**
 * Reads the current value of a workflow's state cell, decoded through the
 * cell's schema. Returns `Option.none` when the workflow has not published
 * the cell. Works on closed workflows too, as Temporal serves queries after
 * completion.
 *
 * @since 1.0.0
 * @category State cells
 */
export const readStateCell = <Value extends Schema.Top>(
  cell: TemporalStateCell<Value>,
  target: WorkflowTarget
): Effect.Effect<
  Option.Option<Value["Type"]>,
  TemporalClientError,
  TemporalClient.TemporalWorkflowClient
> =>
  Effect.gen(function*() {
    const client = yield* TemporalClient.TemporalWorkflowClient
    const result = yield* client.getHandle(
      workflowIdFor(target.workflow._tag, target.executionId, target.workflowIdPrefix)
    ).query<TemporalStateCellResult, [string]>(stateCellQueryName, cell.name)
    return result.found
      ? Option.some(cell.codecs.decode(result.value) as Value["Type"])
      : Option.none()
  })

/**
 * Offers a schema-typed message to a running workflow's durable mailbox. The
 * message is validated and encoded before the signal is sent.
 *
 * @since 1.0.0
 * @category Mailboxes
 */
export const offerMailbox = <Payload extends Schema.Top>(
  mailbox: TemporalDurableMailbox<Payload>,
  target: WorkflowTarget,
  message: Payload["Type"]
): Effect.Effect<void, TemporalClientError, TemporalClient.TemporalWorkflowClient> =>
  Effect.gen(function*() {
    const client = yield* TemporalClient.TemporalWorkflowClient
    yield* client.getHandle(
      workflowIdFor(target.workflow._tag, target.executionId, target.workflowIdPrefix)
    ).signal(
      mailboxSignalName,
      {
        name: mailbox.name,
        payload: mailbox.codecs.encode(message)
      } satisfies MailboxSignal
    )
  })

/**
 * The update response did not arrive within the configured timeout. The
 * request may still be served later; retry with the same `requestId` to poll
 * for it idempotently.
 *
 * @since 1.0.0
 * @category Errors
 */
export class UpdateTimeoutError extends Schema.TaggedError<UpdateTimeoutError>()("UpdateTimeoutError", {
  updateName: Schema.String,
  requestId: Schema.String,
  timeoutMillis: Schema.Number
}) {}

/**
 * Options for `executeUpdate`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface ExecuteUpdateOptions {
  /**
   * Correlates the request with its response; pass a stable id to make the
   * update idempotent across retries. Defaults to a random UUID.
   */
  readonly requestId?: string | undefined
  readonly pollIntervalMillis?: number | undefined
  readonly timeoutMillis?: number | undefined
}

/**
 * Sends a schema-typed update request to a running workflow and awaits the
 * typed response: the update's success lands in the success channel, its
 * schema'd failure in the error channel.
 *
 * @since 1.0.0
 * @category Updates
 */
export const executeUpdate = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top
>(
  update: TemporalDurableUpdate<Payload, Success, Error>,
  target: WorkflowTarget,
  payload: Payload["Type"],
  options?: ExecuteUpdateOptions | undefined
): Effect.Effect<
  Success["Type"],
  Error["Type"] | TemporalClientError | UpdateTimeoutError,
  TemporalClient.TemporalWorkflowClient
> =>
  Effect.gen(function*() {
    const client = yield* TemporalClient.TemporalWorkflowClient
    const requestId = options?.requestId ?? crypto.randomUUID()
    const pollIntervalMillis = options?.pollIntervalMillis ?? 100
    const timeoutMillis = options?.timeoutMillis ?? 30_000
    const handle = client.getHandle(
      workflowIdFor(target.workflow._tag, target.executionId, target.workflowIdPrefix)
    )

    yield* handle.signal(
      updateSignalName,
      {
        name: update.name,
        requestId,
        payload: update.payloadCodecs.encode(payload)
      } satisfies UpdateSignal
    )

    const deadline = Date.now() + timeoutMillis
    while (Date.now() < deadline) {
      const result = yield* handle.query<TemporalUpdateResult, [string]>(updateResultQueryName, requestId)
      if (result.found) {
        const exit = update.exitCodecs.decode(result.exit) as Exit.Exit<Success["Type"], Error["Type"]>
        return yield* exit
      }
      yield* Effect.sleep(pollIntervalMillis)
    }
    return yield* new UpdateTimeoutError({ requestId, timeoutMillis, updateName: update.name })
  })
