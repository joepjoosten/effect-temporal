/**
 * Driving the workflows from ordinary Node: typed execute, observing
 * published state, completing approvals, entity interactions.
 *
 * Run alongside the worker: `pnpm tsx sample/order-saga/client.ts`.
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as TemporalClient from "../../src/TemporalClient.js"
import * as TemporalConnection from "../../src/TemporalConnection.js"
import * as TemporalWorkflowEngine from "../../src/TemporalWorkflowEngine.js"
import * as TemporalWorkflowInteractions from "../../src/TemporalWorkflowInteractions.js"
import {
  managerApproval,
  orderStatus,
  orderWorkflow,
  setPlan,
  subscriptionCommands,
  subscriptionStatus,
  subscriptionWorkflow
} from "./definitions.js"

const temporalLayer = TemporalWorkflowEngine.layer({ taskQueue: "order-saga" }).pipe(
  Layer.provideMerge(TemporalClient.layer()),
  Layer.provideMerge(TemporalConnection.layer({ address: "localhost:7233" }))
)

const program = Effect.gen(function*() {
  // -------------------------------------------------------------------------
  // Order saga: start without waiting, watch its published status, approve it,
  // then collect the typed result. A ChargeDeclined would land in the error
  // channel as a real class instance.
  // -------------------------------------------------------------------------
  const order = { amountCents: 4200, orderId: "ord_123" }
  const orderTarget = {
    executionId: yield* orderWorkflow.executionId(order),
    workflow: orderWorkflow
  }

  yield* orderWorkflow.execute(order, { discard: true })

  const status = yield* TemporalWorkflowInteractions.readStateCell(orderStatus, orderTarget)
  yield* Effect.log("order status", status)

  const token = yield* DurableDeferred.tokenFromPayload(managerApproval, {
    payload: order,
    workflow: orderWorkflow
  })
  yield* DurableDeferred.succeed(managerApproval, {
    token,
    value: { approverId: "boss" }
  })

  // Attaches to the running (or already completed) execution by digest id.
  const receipt = yield* orderWorkflow.execute(order)
  yield* Effect.log("order complete", receipt)

  // -------------------------------------------------------------------------
  // Subscription entity: typed updates, mailbox commands, queryable state.
  // -------------------------------------------------------------------------
  const subscription = { billedCycles: 0, customerId: "cus_1", plan: "starter" }
  const subscriptionTarget = {
    executionId: yield* subscriptionWorkflow.executionId(subscription),
    workflow: subscriptionWorkflow
  }

  yield* subscriptionWorkflow.execute(subscription, { discard: true })

  // Request/response with typed success and typed failure channels.
  const upgraded = yield* TemporalWorkflowInteractions.executeUpdate(setPlan, subscriptionTarget, { plan: "pro" })
  yield* Effect.log("plan is now", upgraded.plan)

  // Fire-and-forget commands into the entity's mailbox.
  yield* TemporalWorkflowInteractions.offerMailbox(subscriptionCommands, subscriptionTarget, { command: "bill" })
  yield* TemporalWorkflowInteractions.offerMailbox(subscriptionCommands, subscriptionTarget, { command: "cancel" })

  const finalState = yield* TemporalWorkflowInteractions.readStateCell(subscriptionStatus, subscriptionTarget)
  yield* Effect.log("subscription state", finalState)

  return yield* subscriptionWorkflow.execute(subscription)
})

Effect.runPromise(
  Effect.scoped(Effect.provide(program, temporalLayer))
).then(console.log, console.error)
