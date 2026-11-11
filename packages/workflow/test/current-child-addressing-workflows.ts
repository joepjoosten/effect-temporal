import * as Runtime from "../src/TemporalWorkflowRuntime.js"
import { child, parent } from "./child-addressing-workflows.js"

export { AddressableChild } from "./child-addressing-workflows.js"

// The current runtime with default options, awaiting the child whatever the
// payload's `discard` flag says.
export const AddressingParent = Runtime.makeWorkflow({
  workflow: parent,
  execute: (payload: { id: string; discard: boolean }) => child.execute({ id: payload.id })
})
