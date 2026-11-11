import { executeChild, patched } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import * as Protocol from "../src/TemporalWorkflowProtocol.js"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"
import { child, parent } from "./child-addressing-workflows.js"

// Reproduce the wire commands of releases that predate the child
// execution-id patch marker: a prefixed child ID behind the
// `child-workflow-ids-v1` marker only. `discard` selects the digest: `true`
// records the pre-`effect@4.0.0-rc.118` id (0.1.x), `false` the current one
// (0.2.0 and 0.3.0).
export const AddressingParent = Runtime.makeWorkflow({
  workflow: parent,
  execute: (payload: { id: string; discard: boolean }) =>
    Effect.gen(function*() {
      patched("effect-temporal/child-workflow-ids-v1")
      const executionId = payload.discard
        ? yield* Protocol.legacyExecutionId(child, { id: payload.id })
        : yield* child.executionId({ id: payload.id })
      return yield* Effect.promise(() =>
        executeChild("AddressableChild", {
          workflowId: Protocol.workflowIdFor(child._tag, executionId),
          args: [{ id: payload.id }]
        })
      )
    })
})

export const AddressableChild = async (): Promise<string> => "approved"
