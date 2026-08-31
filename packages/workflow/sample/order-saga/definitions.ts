/**
 * Shared, sandbox-safe definitions: one schema'd definition per workflow,
 * activity, or message channel, imported by the workflow bundle, the worker,
 * and every client — the sides cannot drift.
 *
 * In an application these imports would be
 * `@effect-temporal/workflow/TemporalTypedActivity` etc.
 */
import * as Schema from "effect/Schema"
import * as DurableDeferred from "effect/unstable/workflow/DurableDeferred"
import * as Workflow from "effect/unstable/workflow/Workflow"
import * as TemporalDurableMailbox from "../../src/TemporalDurableMailbox.js"
import * as TemporalDurableUpdate from "../../src/TemporalDurableUpdate.js"
import * as TemporalNexusOperation from "../../src/TemporalNexusOperation.js"
import * as TemporalStateCell from "../../src/TemporalStateCell.js"
import * as TemporalTypedActivity from "../../src/TemporalTypedActivity.js"

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class ChargeDeclined extends Schema.TaggedError<ChargeDeclined>()("ChargeDeclined", {
  code: Schema.Number
}) {}

export class QuoteRejected extends Schema.TaggedError<QuoteRejected>()("QuoteRejected", {
  reason: Schema.String
}) {}

export class InvalidPlan extends Schema.TaggedError<InvalidPlan>()("InvalidPlan", {
  plan: Schema.String
}) {}

// ---------------------------------------------------------------------------
// Order saga (one-shot workflow)
// ---------------------------------------------------------------------------

export const orderWorkflow = Workflow.make("OrderWorkflow", {
  payload: {
    orderId: Schema.String,
    amountCents: Schema.Number
  },
  success: Schema.String,
  error: ChargeDeclined,
  idempotencyKey: ({ orderId }) => orderId
})

/** Typed activity: payload, success, and failure are schemas; options ride along. */
export const chargeCard = TemporalTypedActivity.make("chargeCard", {
  payload: Schema.Struct({ orderId: Schema.String, amountCents: Schema.Number }),
  success: Schema.Struct({ receiptId: Schema.String }),
  error: ChargeDeclined,
  options: { retry: { maximumAttempts: 3 }, startToCloseTimeout: "30 seconds" }
})

export const refundCharge = TemporalTypedActivity.make("refundCharge", {
  payload: Schema.Struct({ receiptId: Schema.String }),
  options: { startToCloseTimeout: "30 seconds" }
})

/** Completed from the outside (a human approval UI, another service, ...). */
export const managerApproval = DurableDeferred.make("manager-approval", {
  success: Schema.Struct({ approverId: Schema.String })
})

/** Observable progress, queryable while running and after the run closes. */
export const orderStatus = TemporalStateCell.make("order-status", {
  value: Schema.Struct({ phase: Schema.String })
})

/** Child workflow used by the saga to arrange shipping. */
export const shipmentWorkflow = Workflow.make("ShipmentWorkflow", {
  payload: {
    orderId: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ orderId }) => orderId
})

// ---------------------------------------------------------------------------
// Subscription (long-lived entity workflow)
// ---------------------------------------------------------------------------

export const subscriptionWorkflow = Workflow.make("SubscriptionWorkflow", {
  payload: {
    customerId: Schema.String,
    billedCycles: Schema.Number,
    plan: Schema.String
  },
  success: Schema.String,
  idempotencyKey: ({ customerId }) => customerId
})

/** Repeated inbound messages (fire-and-forget commands). */
export const subscriptionCommands = TemporalDurableMailbox.make("subscription-commands", {
  payload: Schema.Struct({ command: Schema.Literals(["bill", "cancel"]) })
})

/** Typed request/response: callers get the new plan back — or a typed failure. */
export const setPlan = TemporalDurableUpdate.make("set-plan", {
  payload: Schema.Struct({ plan: Schema.String }),
  success: Schema.Struct({ plan: Schema.String }),
  error: InvalidPlan
})

export const subscriptionStatus = TemporalStateCell.make("subscription-status", {
  value: Schema.Struct({ billedCycles: Schema.Number, plan: Schema.String })
})

// ---------------------------------------------------------------------------
// Nexus: expose the quote workflow as a typed cross-namespace operation
// ---------------------------------------------------------------------------

export const quoteWorkflow = Workflow.make("QuoteWorkflow", {
  payload: {
    sku: Schema.String,
    quantity: Schema.Number
  },
  success: Schema.Struct({ totalCents: Schema.Number }),
  error: QuoteRejected,
  idempotencyKey: ({ quantity, sku }) => `${sku}/${quantity}`
})

export const getQuote = TemporalNexusOperation.make("quote-service", "getQuote", {
  payload: Schema.Struct({ sku: Schema.String, quantity: Schema.Number }),
  success: Schema.Struct({ totalCents: Schema.Number }),
  error: QuoteRejected
})
