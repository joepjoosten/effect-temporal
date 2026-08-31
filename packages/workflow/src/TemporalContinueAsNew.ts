/**
 * Continue-as-new support for long-lived Effect workflows.
 *
 * Entity-style workflows accumulate history indefinitely; Temporal's answer
 * is continue-as-new: close the current run and start a fresh one under the
 * same workflow id with new arguments. `continueAsNew` encodes the new
 * payload with the workflow's schema and triggers Temporal's continuation —
 * the effect never returns. The runtime lets the continuation marker escape
 * the Effect runtime untouched, and clients awaiting the execution follow the
 * run chain to the final result.
 *
 * This module is sandbox-safe.
 *
 * @since 1.0.0
 */
import { makeContinueAsNewFunc } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import type * as Workflow from "effect/unstable/workflow/Workflow"
import { wireCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * Options for `continueAsNew`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface ContinueAsNewOptions {
  readonly memo?: Record<string, unknown> | undefined
}

/**
 * Closes the current run and continues the workflow as a new run with the
 * given payload. Never returns; per-run state (mailbox logs, update logs,
 * state cells) starts fresh in the new run.
 *
 * @since 1.0.0
 * @category Combinators
 */
export const continueAsNew = <
  Name extends string,
  Payload extends Workflow.AnyStructSchema,
  Success extends Schema.Top,
  Error extends Schema.Top
>(
  workflow: Workflow.Workflow<Name, Payload, Success, Error>,
  payload: Payload["Type"],
  options?: ContinueAsNewOptions | undefined
): Effect.Effect<never> =>
  Effect.promise(() =>
    makeContinueAsNewFunc(options?.memo === undefined ? {} : { memo: options.memo })(
      wireCodecsFor(workflow).encodePayload(payload)
    )
  ) as Effect.Effect<never>
