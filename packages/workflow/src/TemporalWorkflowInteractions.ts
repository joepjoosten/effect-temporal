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
import * as Option from "effect/Option"
import type * as Schema from "effect/Schema"
import type * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalClient from "./TemporalClient.js"
import type { TemporalClientError } from "./TemporalError.js"
import type { TemporalStateCell } from "./TemporalStateCell.js"
import { stateCellQueryName, type TemporalStateCellResult, workflowIdFor } from "./TemporalWorkflowProtocol.js"

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
