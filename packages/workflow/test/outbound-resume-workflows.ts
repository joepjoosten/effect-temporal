import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as Mailbox from "../src/TemporalDurableMailbox.js"
import * as Runtime from "../src/TemporalWorkflowRuntime.js"

export const approval = DurableDeferred.make("outbound-approval", { success: Schema.String })
export const messages = Mailbox.make("outbound-messages", { payload: Schema.String })
export const receiver = Workflow.make("OutboundReceiver", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
export const sender = Workflow.make("OutboundSender", {
  payload: { id: Schema.String },
  success: Schema.String,
  idempotencyKey: ({ id }) => id
})
export const OutboundReceiver = Runtime.makeWorkflow({
  workflow: receiver,
  execute: () =>
    Effect.gen(function*() {
      const values: Array<string> = []
      for (let i = 0; i < 3; i++) values.push(yield* Mailbox.take(messages))
      return values.join(",")
    })
})
export const OutboundSender = Runtime.makeWorkflow({
  workflow: sender,
  execute: (payload: { id: string }) =>
    Effect.gen(function*() {
      const target = { workflow: receiver, executionId: yield* receiver.executionId(payload) }
      yield* Mailbox.offer(messages, target, "first")
      yield* Mailbox.offer(messages, target, "second")
      yield* DurableDeferred.await(approval)
      yield* Mailbox.offer(messages, target, "after")
      return "sent"
    })
})
