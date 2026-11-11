import * as Runtime from "../src/TemporalWorkflowRuntime.js"
import { child, parent } from "./child-addressing-workflows.js"

export { AddressableChild } from "./child-addressing-workflows.js"

// The current runtime, opted in to replaying pre-marker child starts under
// the pre-`effect@4.0.0-rc.118` execution id.
export const AddressingParent = Runtime.makeWorkflow({
  workflow: parent,
  execute: (payload: { id: string; discard: boolean }) => child.execute({ id: payload.id }),
  legacyChildExecutionIds: true
})
