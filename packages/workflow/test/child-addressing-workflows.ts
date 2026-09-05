import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as StateCell from "../src/TemporalStateCell.js"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"

export const approval = DurableDeferred.make("child-approval", { success: Schema.String })
export const parentDone = DurableDeferred.make("parent-done", { success: Schema.String })
export const status = StateCell.make("child-status", { value: Schema.String })
export const child = Workflow.make("AddressableChild", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
const makeParent = (name: string) =>
  Workflow.make(name, {
    payload: { id: Schema.String, discard: Schema.Boolean },
    success: Schema.String,
    idempotencyKey: ({ id }) => id
  })
export const parent = makeParent("AddressingParent")
export const prefixedParent = makeParent("PrefixedAddressingParent")
const execute = (payload: { id: string; discard: boolean }) =>
  Effect.gen(function*() {
    if (payload.discard) {
      yield* child.execute({ id: payload.id }, { discard: true })
      return yield* DurableDeferred.await(parentDone)
    }
    return yield* child.execute({ id: payload.id })
  })
export const AddressableChild = Runtime.makeWorkflow({
  workflow: child,
  execute: () =>
    Effect.gen(function*() {
      yield* StateCell.set(status, "waiting")
      return yield* DurableDeferred.await(approval)
    })
})
export const AddressingParent = Runtime.makeWorkflow({ workflow: parent, execute })
export const PrefixedAddressingParent = Runtime.makeWorkflow({
  workflow: prefixedParent,
  execute,
  workflowIdPrefix: "custom"
})
