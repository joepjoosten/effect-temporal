import { executeChild } from "@temporalio/workflow"
import * as Effect from "effect/Effect"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"
import { child, parent } from "./child-addressing-workflows.js"

// Reproduce the pre-fix wire commands: an unprefixed child ID and no patch marker.
export const AddressingParent = Runtime.makeWorkflow({
  workflow: parent,
  execute: (payload: { id: string; discard: boolean }) =>
    Effect.gen(function*() {
      const workflowId = yield* child.executionId(payload)
      return yield* Effect.promise(() =>
        executeChild("AddressableChild", {
          workflowId,
          args: [{ id: payload.id }]
        })
      )
    })
})

export const AddressableChild = async (): Promise<string> => "approved"
