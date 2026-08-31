/**
 * The workflow bundle — the file a worker loads as `workflowsPath`. It runs
 * inside Temporal's deterministic sandbox, so it may only import sandbox-safe
 * modules (definitions, `effect`, and the runtime/primitive modules). The
 * `TemporalLint` rules enforce this.
 *
 * Bodies are ordinary Effect programs; every export created by
 * `makeWorkflow` is a Temporal workflow function.
 */
import * as Effect from "effect/Effect"
import * as DurableClock from "effect/unstable/workflow/DurableClock"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as TemporalContinueAsNew from "../../src/TemporalContinueAsNew.js"
import * as TemporalDurableMailbox from "../../src/TemporalDurableMailbox.js"
import * as TemporalDurableUpdate from "../../src/TemporalDurableUpdate.js"
import * as TemporalStateCell from "../../src/TemporalStateCell.js"
import * as TemporalTypedActivity from "../../src/TemporalTypedActivity.js"
import * as TemporalVersioning from "../../src/TemporalVersioning.js"
import * as TemporalWorkflowRuntime from "../../src/TemporalWorkflowRuntime.js"
import {
  chargeCard,
  InvalidPlan,
  managerApproval,
  orderStatus,
  orderWorkflow,
  QuoteRejected,
  quoteWorkflow,
  refundCharge,
  setPlan,
  shipmentWorkflow,
  subscriptionCommands,
  subscriptionStatus,
  subscriptionWorkflow
} from "./definitions.js"

// ---------------------------------------------------------------------------
// One-shot saga: typed activities, compensation, durable timer, approval
// deferred, published status, child workflow, versioning
// ---------------------------------------------------------------------------

export const OrderWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: orderWorkflow,
  execute: (payload: { readonly orderId: string; readonly amountCents: number }) =>
    Effect.gen(function*() {
      yield* TemporalStateCell.set(orderStatus, { phase: "charging" })

      // A typed activity call: the schema'd ChargeDeclined lands in the error
      // channel; the compensation refunds the charge if anything later fails.
      const receipt = yield* orderWorkflow.withCompensation(
        TemporalTypedActivity.call(chargeCard, payload),
        (charged) => TemporalTypedActivity.call(refundCharge, { receiptId: charged.receiptId })
      )

      // A durable timer: survives worker restarts, skips instantly in the
      // time-skipping test server.
      yield* TemporalStateCell.set(orderStatus, { phase: "cooling-off" })
      yield* DurableClock.sleep({ name: "cooling-off", duration: "1 minute" })

      // Suspend until a human (or another system) completes the deferred.
      yield* TemporalStateCell.set(orderStatus, { phase: "awaiting-approval" })
      const approval = yield* DurableDeferred.await(managerApproval)

      // Patch-marker versioning: new executions take v2, histories recorded
      // by v1 deployments keep replaying their branch.
      const shipment = yield* TemporalVersioning.match({
        versions: [
          { id: "ship-v1", run: Effect.succeed("legacy-carrier") },
          { id: "ship-v2", run: shipmentWorkflow.execute({ orderId: payload.orderId }) }
        ]
      })

      yield* TemporalStateCell.set(orderStatus, { phase: "done" })
      return `${receipt.receiptId}:${approval.approverId}:${shipment}`
    })
})

export const ShipmentWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: shipmentWorkflow,
  execute: (payload: { readonly orderId: string }) => Effect.succeed(`shipment-${payload.orderId}`)
})

// ---------------------------------------------------------------------------
// Entity workflow: mailbox commands, typed updates, published state,
// continue-as-new to keep history bounded
// ---------------------------------------------------------------------------

const CYCLES_PER_RUN = 50

export const SubscriptionWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: subscriptionWorkflow,
  execute: (payload: { readonly customerId: string; readonly billedCycles: number; readonly plan: string }) =>
    Effect.gen(function*() {
      let billedCycles = payload.billedCycles
      let plan = payload.plan

      while (true) {
        yield* TemporalStateCell.set(subscriptionStatus, { billedCycles, plan })

        // Serve whichever arrives first: a command or a plan-change request.
        const event = yield* Effect.raceAll([
          TemporalDurableMailbox.take(subscriptionCommands).pipe(
            Effect.map((message) => ({ _tag: "command" as const, message }))
          ),
          TemporalDurableUpdate.take(setPlan).pipe(
            Effect.map((request) => ({ _tag: "update" as const, request }))
          )
        ])

        if (event._tag === "update") {
          if (event.request.payload.plan === "") {
            yield* event.request.fail(new InvalidPlan({ plan: event.request.payload.plan }))
          } else {
            plan = event.request.payload.plan
            yield* event.request.succeed({ plan })
          }
          continue
        }

        if (event.message.command === "cancel") {
          return `${payload.customerId}:cancelled-after:${billedCycles}`
        }
        billedCycles += 1

        // Bound the history: roll the accumulated state into a fresh run.
        if (billedCycles % CYCLES_PER_RUN === 0) {
          return yield* TemporalContinueAsNew.continueAsNew(subscriptionWorkflow, {
            billedCycles,
            customerId: payload.customerId,
            plan
          })
        }
      }
    })
})

// ---------------------------------------------------------------------------
// Nexus-backed workflow (started by the Nexus handler on the worker)
// ---------------------------------------------------------------------------

export const QuoteWorkflow = TemporalWorkflowRuntime.makeWorkflow({
  workflow: quoteWorkflow,
  execute: (payload: { readonly sku: string; readonly quantity: number }) =>
    payload.quantity <= 0
      ? Effect.fail(new QuoteRejected({ reason: `invalid quantity ${payload.quantity}` }))
      : Effect.succeed({ totalCents: payload.quantity * 100 })
})
