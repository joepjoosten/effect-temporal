/**
 * Worker-side Nexus operation handlers backed by Effect workflows.
 *
 * `workflowRunOperation` turns a `TemporalNexusOperation` definition plus an
 * Effect workflow into a Nexus service handler for the worker's
 * `nexusServices` option: incoming requests are schema-decoded, the backing
 * workflow starts under its deterministic execution id (attaching to an
 * existing open run on duplicate requests), and its result — success or
 * typed failure — flows back to the caller as the operation outcome.
 *
 * This module uses `@temporalio/nexus` and must not be imported from a
 * workflow bundle.
 *
 * @since 1.0.0
 */
import { startWorkflow, WorkflowRunOperationHandler } from "@temporalio/nexus"
import * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import type * as Workflow from "effect/unstable/workflow/Workflow"
import * as NexusRpc from "nexus-rpc"
import type { TemporalNexusOperation } from "./TemporalNexusOperation.js"
import { workflowIdFor } from "./TemporalWorkflowProtocol.js"
import { wireCodecsFor } from "./TemporalWorkflowWire.js"

/**
 * Options for `workflowRunOperation`.
 *
 * @since 1.0.0
 * @category Models
 */
export interface WorkflowRunOperationOptions {
  /**
   * Must match the `workflowIdPrefix` of the engine used to observe the
   * backing workflow, when one is configured.
   */
  readonly workflowIdPrefix?: string | undefined
}

/**
 * Builds a Nexus service handler that backs the operation with the given
 * Effect workflow. Duplicate requests for the same execution id attach to
 * the existing run (`USE_EXISTING` conflict policy), keeping the operation
 * idempotent by payload digest.
 *
 * @since 1.0.0
 * @category Constructors
 */
export const workflowRunOperation = <
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
  Name extends string,
  WorkflowPayload extends Workflow.AnyStructSchema
>(
  operation: TemporalNexusOperation<Payload, Success, Error>,
  workflow: Workflow.Workflow<Name, WorkflowPayload, Success, Error>,
  options?: WorkflowRunOperationOptions | undefined
): NexusRpc.ServiceHandler<any> =>
  NexusRpc.serviceHandler(operation.service, {
    [operation.operationName]: new WorkflowRunOperationHandler<unknown, unknown>(async (ctx, input) => {
      const payload = operation.payloadCodecs.decode(input)
      const executionId = await Effect.runPromise(workflow.executionId(payload))
      return await startWorkflow(ctx, workflow._tag, {
        args: [wireCodecsFor(workflow).encodePayload(payload)],
        workflowId: workflowIdFor(workflow._tag, executionId, options?.workflowIdPrefix),
        workflowIdConflictPolicy: "USE_EXISTING"
      })
    })
  })
